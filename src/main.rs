use mysql::*;
use mysql::prelude::*;
use chrono::NaiveDateTime;
use std::{fmt, fs};
use std::time::Instant;
use std::ops::Deref;
use std::path::Path;
use path_absolutize::Absolutize;
use shellexpand;

// ***************************************************************************
//                                Constants
// ***************************************************************************
// The file that contains the complete mysql database url, including password.
const DB_URL_FILE: &str = "~/smartsched-db.url";
const DEFAULT_OUTPUT_TABLE: &str = "jobq_history";
const DEFAULT_COMMIT_BATCH_SIZE: i32 = 10000;

// ***************************************************************************
//                                  Structs
// ***************************************************************************
// ---------------------------------------------------------------------------
// DBsource:
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct DBsource {
    pub jobid: String,
    pub submit: NaiveDateTime,
    pub start: NaiveDateTime,
    pub max_minutes: i32,
    pub queue_minutes: i32,
}

impl DBsource {
    fn new(jobid: String, submit: NaiveDateTime, start: NaiveDateTime, max_minutes: i32, queue_minutes: i32) -> DBsource {
        DBsource { jobid, submit, start, max_minutes, queue_minutes}
    }
}

impl fmt::Display for DBsource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}, {}, {}, {}", self.jobid, self.submit, self.start, self.max_minutes, self.queue_minutes)
    }
}

// ---------------------------------------------------------------------------
// DBtarget:
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct DBtarget {
    pub jobid: String,
    pub submit: NaiveDateTime,
    pub start: NaiveDateTime,
    pub max_minutes: i32,
    pub queue_minutes: i32,
    pub backlog_minutes: i32,
    pub backlog_num_jobs: i32,
}

impl DBtarget {
    fn new(src: DBsource, backlog_minutes: i32, backlog_num_jobs: i32) -> DBtarget {
        DBtarget {jobid: src.jobid, submit: src.submit, start: src.start, max_minutes: src.max_minutes, queue_minutes: src.queue_minutes,
                  backlog_minutes, backlog_num_jobs}
    }
}

impl fmt::Display for DBtarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}, {}, {}, {}, {}, {}", self.jobid, self.submit, self.start, self.max_minutes, self.queue_minutes,
                                                self.backlog_minutes, self.backlog_num_jobs)
    }
}

// ***************************************************************************
//                                 Functions
// ***************************************************************************
// ---------------------------------------------------------------------------
// main:
// ---------------------------------------------------------------------------
/** This program plays back the history of a set of jobs to recreate the backlog
 * of jobs at the time of each job's submission.  We calculate the total time
 * requested by the jobs in the backlog and the number of jobs in the backlog
 * for each job. 
 * 
 * Currently, we just print the results to the console, but the ultimate  
 * destination for this information would a database.
 */
fn main() {
    // Connection information including password.
    let url = load_db_url();
    
    // Adjust query as needed.
    let select = "SELECT jobid, submit, start, max_minutes, \
                       TIMESTAMPDIFF(MINUTE, submit, start) as queue_minutes \
                       FROM stampede2 \
                       WHERE queue = 'normal' AND state = 'COMPLETED' \
                       ORDER BY submit ASC";

    // Connect to the database and panic if we can't.
    let pool = Pool::new(url.as_str()).expect("Failed to create pool.");
    let mut conn1 = pool.get_conn().expect("Failed to create conn1.");
    let mut conn2 = pool.get_conn().expect("Failed to create conn2.");

    // Create the output table using the first connection.
    // Prepare the insert statement on the second connection.
    create_output_table(&mut conn1, DEFAULT_OUTPUT_TABLE);
    let insert_stmt = get_insert_stmt(&mut conn2, DEFAULT_OUTPUT_TABLE);

    // Create the queue with a capacity not likely to be exceeded.
    let mut backlog_queue: Vec<DBsource> = Vec::with_capacity(256);
    let mut output_records: Vec<DBtarget> =  Vec::with_capacity(4000000);

    // Set up the database download.
    let mut num_rows = 0;
    let mut num_row_err: i32 = 0;
    let before = Instant::now();

    // Iterate through the records one by one.
    println!("\nIssuing query: {}", select);
    conn1.query_iter(select).expect("query_iter() failure").for_each(|row| {
        match row {
            Ok(a) => {
                // Decode the database row.
                let r = from_row::<(String, String, String, i32, i32)>(a);
                let cur_job = DBsource::new(
                    r.0,
                    NaiveDateTime::parse_from_str(r.1.as_str(), "%Y-%m-%d %H:%M:%S").expect("Submit decode failure."),
                    NaiveDateTime::parse_from_str(r.2.as_str(), "%Y-%m-%d %H:%M:%S").expect("Start decode failure."),
                    r.3,
                    r.4,
                );

                // Set up for analyzing the current job's queue.
                let mut keep: Vec<bool> = Vec::with_capacity(backlog_queue.len());
                let mut cur_backlog_len = 0;
                let mut cur_queue_minutes = 0;

                // Iterate through existing backlog queue looking for jobs whose start
                // time has not occurred when the current job was submitted.
                for backlog_job in &backlog_queue {
                    if backlog_job.start > cur_job.submit {
                        cur_backlog_len += 1;
                        cur_queue_minutes += backlog_job.max_minutes;
                        keep.push(true); // This element stays in queue.
                    } else {
                        // Mark current queue element for removal.
                        keep.push(false);
                    }
                }

                // Remove the backlog jobs marked for removal.
                let mut iter = keep.iter();
                backlog_queue.retain(|_| *iter.next().expect("backlog.queue.retain failure"));

                // Push the current ob onto the backlog queue.
                backlog_queue.push(cur_job.clone());

                // Create the current job's output record.
                let cur_job_output = DBtarget::new(cur_job, cur_queue_minutes, cur_backlog_len);           
                output_records.push(cur_job_output);
                
                // Print a message every time we commit a group of output records.
                num_rows += 1;
                if (num_rows % DEFAULT_COMMIT_BATCH_SIZE) == 0 {
                    println!("Database rows read = {}.", num_rows);
                }
            },
            Err(e) => {
                num_rows += 1; num_row_err += 1;
                println!("Error reading row {}: {}", num_rows, e);
            },
        };
    });

    // Commit any residual rows.
    println!("Database rows read = {}.", num_rows);
    conn1.query_drop("COMMIT").expect("Final commit on conn1 failure.");

    // Write all 
    let write_result = write_output(&mut conn2, &insert_stmt, output_records);

    // Create indexes after all rows loaded.
    create_indexes(&mut conn2, DEFAULT_OUTPUT_TABLE);

    // Print results.
    println!("\nElapsed time: {:.2?}", before.elapsed());
    println!("Total rows read  = {}", num_rows);
    println!("Total row read errors = {}", num_row_err);
    println!("Total rows written = {}", write_result.0);
    println!("Total row write errors = {}", write_result.1);

}

// ---------------------------------------------------------------------------
// load_db_url:
// ---------------------------------------------------------------------------
fn load_db_url() -> String {
    fs::read_to_string(get_absolute_path(DB_URL_FILE)).expect("Failed to load mysql url from file.")
}

// ---------------------------------------------------------------------------
// create_output_table:
// ---------------------------------------------------------------------------
fn create_output_table(conn: &mut PooledConn, table_name: &str) {

    // Drop the output table if it already exists.
    conn.query_drop("DROP TABLE IF EXISTS ".to_owned() + table_name).expect("Drop table failure.");

    // Create a new table.
    let create_table = "CREATE TABLE :table_name \
                             (jobid varchar(30) NOT NULL PRIMARY KEY, \
                             submit datetime NOT NULL, \
                             start datetime NOT NULL, \
                             max_minutes int unsigned NOT NULL, \
                             queue_minutes int unsigned NOT NULL, \
                             backlog_minutes int unsigned NOT NULL, \
                             backlog_num_jobs int unsigned NOT NULL)";
    let create_cmd = create_table.replacen(":table_name", table_name, 1);
    conn.query_drop(create_cmd).expect("Create table failure.");
}

// ---------------------------------------------------------------------------
// create_indexes:
// ---------------------------------------------------------------------------
fn create_indexes(conn: &mut PooledConn, table_name: &str) {
    // Create indexes on output table.
    conn.query_drop("CREATE INDEX index_submit ON ".to_owned() + table_name + " (submit)").expect("Create index 1 failure.");
    conn.query_drop("CREATE INDEX index_start ON ".to_owned() + table_name + " (start)").expect("Create index 2 failure.");
    conn.query_drop("CREATE INDEX index_max_minutes ON ".to_owned() + table_name + " (max_minutes)").expect("Create index 3 failure.");
    conn.query_drop("CREATE INDEX index_queue_minutes ON ".to_owned() + table_name + " (queue_minutes)").expect("Create index 4 failure.");
    conn.query_drop("CREATE INDEX index_backlog_minutes ON ".to_owned() + table_name + " (backlog_minutes)").expect("Create index 5 failure.");
    conn.query_drop("CREATE INDEX index_backlog_num_jobs ON ".to_owned() + table_name + " (backlog_num_jobs)").expect("Create index 6 failure.");
}

// ---------------------------------------------------------------------------
// get_insert_stmt:
// ---------------------------------------------------------------------------
fn get_insert_stmt(conn: &mut PooledConn, table_name: &str) -> Statement {
    // Build the insert statement with placeholders.
    let insert = "INSERT INTO :table_name \
            (jobid, submit, start, max_minutes, queue_minutes, backlog_minutes, backlog_num_jobs) \
            VALUES (:jobid, :submit, :start, :max_minutes, :queue_minutes, :backlog_minutes, :backlog_num_jobs)";
    let insert_cmd = insert.replacen(":table_name", table_name, 1);
    let stmt = conn.prep(insert_cmd).expect("Prepare statement failure.");

    // Set automcommit off on this connection so that inserts are committed in bulk.
    conn.query_drop("SET autocommit=0").expect("Set autocommit off failure.");

    stmt        
}

// ---------------------------------------------------------------------------
// write_output:
// ---------------------------------------------------------------------------
fn write_output(conn: &mut PooledConn, stmt: &Statement, recs: Vec<DBtarget>) -> (i32, i32) {

    // Write each output record to the database.
    let mut num_rows = 0;
    let mut num_rows_err = 0;
    println!("\nStarting to write {} output records to the database.", recs.len());
    for rec in recs {
        match conn.exec_drop(stmt, params! {
            "jobid" => rec.jobid,
            "submit" => rec.submit.to_string(),
            "start" =>  rec.start.to_string(),
            "max_minutes" => rec.max_minutes,
            "queue_minutes" => rec.queue_minutes,
            "backlog_minutes" => rec.backlog_minutes,
            "backlog_num_jobs" => rec.backlog_num_jobs,
        }) {
            Ok(_) => {
                num_rows += 1;
                if (num_rows % DEFAULT_COMMIT_BATCH_SIZE) == 0 {
                    println!("Database rows written = {}.", num_rows);
                    conn.query_drop("COMMIT").expect("Batch commit failure.");
                }
            },
            Err(e) => {
                num_rows += 1; num_rows_err += 1;
                println!("Error writing record {} to database: {}", num_rows, e);
            }
        }
    }

    // Commit any residual rows.
    println!("Database rows written = {}.", num_rows);
    conn.query_drop("COMMIT").expect("Final commit on conn2 failure.");

    // Return the stats.
    (num_rows, num_rows_err) 
}

// ---------------------------------------------------------------------------
// insert_output:
// ---------------------------------------------------------------------------
fn insert_output(conn: &mut PooledConn, stmt: &Statement, rec: DBtarget) {
    // Insert a job's record in the output table..
    match conn.exec_drop(stmt, params! {
        "jobid" => rec.jobid,
        "submit" => rec.submit.to_string(),
        "start" =>  rec.start.to_string(),
        "max_minutes" => rec.max_minutes,
        "queue_minutes" => rec.queue_minutes,
        "backlog_minutes" => rec.backlog_minutes,
        "backlog_num_jobs" => rec.backlog_num_jobs,
    }) {
        Ok(_) => (),
        Err(e) => {
            println!("Error writing to database: {}", e);
            ()
        }
    }
}

// ---------------------------------------------------------------------------
// get_absolute_path:
// ---------------------------------------------------------------------------
/** Replace tilde (~) and environment variable values in a path name and
 * then construct the absolute path name.  The difference between 
 * absolutize and standard canonicalize methods is that absolutize does not 
 * care about whether the file exists and what the file really is.
 * 
 * Here's a short version of how canonicalize would be used: 
 * 
 *   let p = shellexpand::full(path).unwrap();
 *   fs::canonicalize(p.deref()).unwrap().into_os_string().into_string().unwrap()
 * 
 * We have the option of using these to two ways to generate a String from the
 * input path (&str):
 * 
 *   path.to_owned()
 *   path.deref().to_string()
 * 
 * I went with the former on a hunch that it's the most appropriate, happy
 * to change if my guess is wrong.
 */
#[allow(dead_code)]
fn get_absolute_path(path: &str) -> String {
    // Replace ~ and environment variable values if possible.
    // On error, return the string version of the original path.
    let s = match shellexpand::full(path) {
        Ok(x) => x,
        Err(_) => return path.to_owned(),
    };

    // Convert to absolute path if necessary.
    // Return original input on error.
    let p = Path::new(s.deref());
    let p1 = match p.absolutize() {
        Ok(x) => x,
        Err(_) => return path.to_owned(),
    };
    let p2 = match p1.to_str() {
        Some(x) => x,
        None => return path.to_owned(),
    };

    p2.to_owned()
}

// ***************************************************************************
//                                 Tests
// ***************************************************************************
#[cfg(test)]
mod tests {
    use mysql::*;
    use mysql::prelude::*;

    #[test]
    fn count_rows() {
        // **** SET EXECUTION PARAMETERS HERE ****
        let table = "stampede2";
        let clause = " WHERE queue = 'normal' AND state = 'COMPLETED'"; // adding state check slowed execution alot
        //let clause = "";

        // Connect to the database.
        let url = "mysql://remoteconnection:19slowBASE!french66@129.114.35.200:3306/HPC_Job_Database";
        //let opts = Opts::from_url(url).unwrap(); 
        let pool = Pool::new(url).unwrap();
        let mut conn = pool.get_conn().unwrap();

        // Issue a query.
        let query = "SELECT count(*) FROM ".to_string() + table + clause;
        //let query = "SELECT count(*) FROM ".to_string() + table;
        let result = conn.query_first(query)
            .map(|row| {row.map(|cnt: u32| cnt)}).unwrap();

        // Print the results.
        match result {
            Some(count) => println!("--> Number of rows in {} = {}", table, count),
            None => println!("No rows found."),
        };
    }
}
