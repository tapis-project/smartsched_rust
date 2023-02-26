use mysql::*;
use mysql::prelude::*;
use chrono::NaiveDateTime;
use std::{fmt, fs};
use std::time::Instant;
use std::ops::Deref;
use std::path::Path;
use std::env;
use lazy_static::lazy_static;
use path_absolutize::Absolutize;

// ***************************************************************************
//                                Constants
// ***************************************************************************
// The file that contains the complete mysql database url, including password.
const DB_URL_FILE: &str = "~/smartsched-db.url";
const DEFAULT_OUTPUT_TABLE: &str = "jobq_history";
const DEFAULT_READ_BATCH_SIZE: i32 = 10000;
const DEFAULT_WRITE_BATCH_SIZE: i32 = 1000;

// ***************************************************************************
//                             Static Variables 
// ***************************************************************************
// Lazily initialize the parameters variable so that is has a 'static lifetime.
// We exit if we can't read our parameters.
lazy_static! {
    // This regex matches "key=value" arguments where key and value are 
        // alphanumeric or underscore and there is no embedded whitespace.
    static ref CONFIG: Config = get_config();
}

// ***************************************************************************
//                                  Structs
// ***************************************************************************
// ---------------------------------------------------------------------------
// Config:
// ---------------------------------------------------------------------------
pub struct Config {
    pub program_pathname: String,
    pub input_table: String,
    pub output_table: String,
    pub ignore_dups: bool,
}

impl Config {
    #[allow(dead_code)]
    fn new(program_pathname: String, input_table: String, output_table: String, ignore_dups: bool) -> Config {
        Config {program_pathname, input_table, output_table, ignore_dups}
    }

    fn println(&self) {
        println!("Input configuration:  input_table={}, output_table={}, ignore_dups={}\n", self.input_table, self.output_table, self.ignore_dups);
    }
}

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
    CONFIG.println();
    let url = load_db_url();
    
    // Adjust query as needed.
    let select1 = "SELECT jobid, submit, start, max_minutes, \
                       TIMESTAMPDIFF(MINUTE, submit, start) as queue_minutes \
                       FROM :input_table \
                       WHERE queue = 'normal' AND state = 'COMPLETED' \
                       ORDER BY submit ASC";
    let select = select1.replacen(":input_table", &CONFIG.input_table, 1);

    // Connect to the database and panic if we can't.
    let pool = Pool::new(url.as_str()).expect("Failed to create pool.");
    let mut conn1 = pool.get_conn().expect("Failed to create conn1.");

    // Create the output table using the first connection.
    create_output_table(&mut conn1);

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
                if (num_rows % DEFAULT_READ_BATCH_SIZE) == 0 {
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

    // Write all output records to the database.
    let write_result = write_output(&mut conn1, output_records);

    // Create indexes after all rows loaded.
    println!("Creating indexes.");
    create_indexes(&mut conn1);

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
// get_config:
// ---------------------------------------------------------------------------
fn get_config() -> Config {
    // Read command-line args.
    let args: Vec<String> = env::args().collect();

    // Initialize local variables.
    let program_pathname  = args[0].clone();
    let mut input_table = "stampede2".to_string();
    let mut output_table  = DEFAULT_OUTPUT_TABLE.to_string();
    let mut ignore_dups = false;

    // Let's inspect the command line for other arguments.
    if args.len() > 1 {
        // The current number of arguments can only be 3 or 5.
        if (args.len() % 2) == 0 {
            panic!("1");
        }

        // Start with the first argument pair.
        let mut index = 1;
        while (index + 1) < args.len() {
            // Get the key and value.
            let key = &args[index];
            let val = &args[index+1];

            // See if they are known arguments.
            // --- Output Table
            if key == "-output_table" {
                output_table = val.clone();
            } 
            // --- Intput Table
            else if key == "-input_table" {
                input_table = val.clone();
            }
            // --- Ignore Duplicate Inserts
            else if key == "-ignore_dups" {
                if val == "true" {
                    ignore_dups = true;
                }
            }
            // --- Abort on Unknown Parameter
            else {
                panic!("2");
            }

            // Increment to next pair.
            index += 2;
        }
    } 

    Config {program_pathname, input_table, output_table, ignore_dups}
}

// ---------------------------------------------------------------------------
// create_output_table:
// ---------------------------------------------------------------------------
fn create_output_table(conn: &mut PooledConn) {

    // Drop the output table if it already exists.
    let table_name = &CONFIG.output_table;
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
fn create_indexes(conn: &mut PooledConn) {
    // Create indexes on output table.
    let table_name = &CONFIG.output_table;
    conn.query_drop("CREATE INDEX index_submit ON ".to_owned() + table_name + " (submit)").expect("Create index 1 failure.");
    conn.query_drop("CREATE INDEX index_start ON ".to_owned() + table_name + " (start)").expect("Create index 2 failure.");
    conn.query_drop("CREATE INDEX index_max_minutes ON ".to_owned() + table_name + " (max_minutes)").expect("Create index 3 failure.");
    conn.query_drop("CREATE INDEX index_queue_minutes ON ".to_owned() + table_name + " (queue_minutes)").expect("Create index 4 failure.");
    conn.query_drop("CREATE INDEX index_backlog_minutes ON ".to_owned() + table_name + " (backlog_minutes)").expect("Create index 5 failure.");
    conn.query_drop("CREATE INDEX index_backlog_num_jobs ON ".to_owned() + table_name + " (backlog_num_jobs)").expect("Create index 6 failure.");
}

// ---------------------------------------------------------------------------
// write_output:
// ---------------------------------------------------------------------------
fn write_output(conn: &mut PooledConn, recs: Vec<DBtarget>) -> (i32, i32) {

    // Prepare the insert statement so it can be reused by the database.
    let stmt = get_insert_stmt(conn);

    // ---- Set automcommit off on this connection so that inserts are committed in bulk.
    conn.query_drop("SET autocommit=0").expect("Set autocommit OFF failure.");

    // Write each output record to the database.
    let mut num_rows = 0;
    let mut num_rows_err = 0;
    println!("\nStarting to write {} output records to the database.", recs.len());
    for rec in recs {
        match conn.exec_drop(&stmt, params! {
            "jobid" => rec.jobid,
            "submit" => rec.submit.to_string(),
            "start" =>  rec.start.to_string(),
            "max_minutes" => rec.max_minutes,
            "queue_minutes" => rec.queue_minutes,
            "backlog_minutes" => rec.backlog_minutes,
            "backlog_num_jobs" => rec.backlog_num_jobs,
        }) {
            Ok(_) => {
                // Commit at a different interval than printing progress messages.
                num_rows += 1;
                if (num_rows % DEFAULT_READ_BATCH_SIZE) == 0 {
                    println!("Database rows written = {}.", num_rows);
                }
                if (num_rows % DEFAULT_WRITE_BATCH_SIZE) == 0 {
                    conn.query_drop("COMMIT").expect("Batch commit failure.");
                }
            },
            Err(e) => {
                num_rows_err += 1;
                println!("Error writing record {} to database: {}", num_rows+1, e);
            }
        }
    }

    // Commit any residual rows.
    println!("Database rows written = {}.", num_rows);
    conn.query_drop("COMMIT").expect("Final commit on conn2 failure.");

    // ---- Set automcommit back on.
    match conn.query_drop("SET autocommit=1") {
        Ok(_) => (),
        Err(e) => {
            println!("Set autocommit ON failure: {}", e);
        },
    };

    // Return the stats.
    (num_rows, num_rows_err) 
}

// ---------------------------------------------------------------------------
// get_insert_stmt:
// ---------------------------------------------------------------------------
fn get_insert_stmt(conn: &mut PooledConn) -> Statement {
    // Assign configuration parameters to placeholder values.
    let ignore = if CONFIG.ignore_dups {
        "IGNORE"
    } else {
        ""
    };
    let table_name = &CONFIG.output_table;

    // Build the insert statement with placeholders filled in.
    let insert1 = "INSERT :ignore INTO :table_name \
            (jobid, submit, start, max_minutes, queue_minutes, backlog_minutes, backlog_num_jobs) \
            VALUES (:jobid, :submit, :start, :max_minutes, :queue_minutes, :backlog_minutes, :backlog_num_jobs)";
    let insert2 = insert1.replacen(":ignore", ignore, 1);
    let insert_cmd = insert2.replacen(":table_name", table_name, 1);
    conn.prep(insert_cmd).expect("Prepare statement failure.")
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

// Add regex 1.7.1 or later to Cargo.toml to run this test.
// #[test]
//     fn test_regex() {
//         let regex = Regex::new("^([[:alnum:]_]+)=([[:alnum:]_]+$)").unwrap();
//         assert!(regex.is_match("abc_d=3"));
//         assert!(regex.is_match("6=3"));
//         assert!(regex.is_match("_abc_d=__"));

//         assert!(!regex.is_match("abc_d =3"));
//         assert!(!regex.is_match("abc_d= 3"));
//         assert!(!regex.is_match("abc_d = 3"));
//         assert!(!regex.is_match("abc_d=3 "));
//         assert!(!regex.is_match(" abc_d=3"));

//         let cap = regex.captures("abc_d=3").unwrap();
//         assert_eq!("abc_d=3".to_string(), cap[0].to_string());
//         assert_eq!("abc_d".to_string(), cap[1].to_string());
//         assert_eq!("3".to_string(), cap[2].to_string());
//     }
