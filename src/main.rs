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
    let pool = Pool::new(url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();

    // Create the output table.
    create_output_table(&mut conn, DEFAULT_OUTPUT_TABLE);

    // Create the queue with a capacity not likely to be exceeded.
    let mut backlog_queue: Vec<DBsource> = Vec::with_capacity(256);

    // Set up the database download.
    let mut num_rows = 0;
    let mut num_row_err: i32 = 0;
    let before = Instant::now();

    // Iterate through the records one by one.
    conn.query_iter(select).unwrap().for_each(|row| {
        match row {
            Ok(a) => {
                // Decode the database row.
                let r = from_row::<(String, String, String, i32, i32)>(a);
                let cur_job = DBsource::new(
                    r.0,
                    NaiveDateTime::parse_from_str(r.1.as_str(), "%Y-%m-%d %H:%M:%S").unwrap(),
                    NaiveDateTime::parse_from_str(r.2.as_str(), "%Y-%m-%d %H:%M:%S").unwrap(),
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
                backlog_queue.retain(|_| *iter.next().unwrap());

                // Push the current ob onto the backlog queue.
                backlog_queue.push(cur_job.clone());

                // Create the current job's output record.
                let cur_job_output = DBtarget::new(cur_job, cur_queue_minutes, cur_backlog_len);           
                
                println!("{}", cur_job_output);
                num_rows += 1;
            },
            Err(_) => {num_rows += 1; num_row_err += 1;},
        };
    });
    println!("Elapsed time: {:.2?}", before.elapsed());
    println!("Total rows read  = {}", num_rows);
    println!("Total row errors = {}", num_row_err);
}

// ---------------------------------------------------------------------------
// load_db_url:
// ---------------------------------------------------------------------------
fn load_db_url() -> String {
    fs::read_to_string(get_absolute_path(DB_URL_FILE)).unwrap()
}

// ---------------------------------------------------------------------------
// create_output_table:
// ---------------------------------------------------------------------------
fn create_output_table(conn: &mut PooledConn, table_name: &str) {

    // Drop the output table if it already exists.
    conn.query_drop("DROP TABLE IF EXISTS ".to_owned() + table_name).unwrap();

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
    conn.query_drop(create_cmd).unwrap();

    // Create indexes on output table.
    conn.query_drop("CREATE INDEX index_submit ON ".to_owned() + table_name + " (submit)").unwrap();
    conn.query_drop("CREATE INDEX index_start ON ".to_owned() + table_name + " (start)").unwrap();
    conn.query_drop("CREATE INDEX index_max_minutes ON ".to_owned() + table_name + " (max_minutes)").unwrap();
    conn.query_drop("CREATE INDEX index_queue_minutes ON ".to_owned() + table_name + " (queue_minutes)").unwrap();
    conn.query_drop("CREATE INDEX index_backlog_minutes ON ".to_owned() + table_name + " (backlog_minutes)").unwrap();
    conn.query_drop("CREATE INDEX index_backlog_num_jobs ON ".to_owned() + table_name + " (backlog_num_jobs)").unwrap();
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
