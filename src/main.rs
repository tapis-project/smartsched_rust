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

//
// SEE HELP MESSAGE FOR USAGE NOTES.
//

// ***************************************************************************
//                                Constants
// ***************************************************************************
// The file that contains the complete mysql database url, including password.
const DB_URL_FILE: &str = "~/smartsched-db.url";
const OUTPUT_TABLE_PREFIX: &str = "jobq_";
const OUTPUT_TABLE_SUFFIX: &str = "";
const INVALID_TABLE: &str = "INVALID_TABLE";
const INVALID_QUEUE: &str = "INVALID_QUEUE";
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
    pub output_table_suffix: String,
    pub queue: String,
    pub ignore_dups: bool,
    pub output_table: String,  // automatically generated (not user specified)
    pub help: bool,
}

impl Config {
    #[allow(dead_code)]
    fn new(program_pathname: String, input_table: String, output_table_suffix: String, queue: String, 
        ignore_dups: bool, output_table: String, help: bool) -> Config {
        Config {program_pathname, input_table, output_table_suffix, queue, ignore_dups, output_table, help}
    }

    fn println(&self) {
        println!("Input configuration:  input_table={}, output_table_suffix={}, queue={}, ignore_dups={}, output_table={}, help={}\n", 
                 self.input_table, self.output_table_suffix, self.queue, self.ignore_dups, self.output_table, self.help);
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
    pub end: NaiveDateTime,
    pub max_minutes: i32,
    pub queue_minutes: i32,
    pub run_minutes: i32,
}

impl DBsource {
    fn new(jobid: String, submit: NaiveDateTime, start: NaiveDateTime, end: NaiveDateTime, 
           max_minutes: i32, queue_minutes: i32, run_minutes: i32) -> DBsource {
        DBsource { jobid, submit, start, end, max_minutes, queue_minutes, run_minutes }
    }
}

impl fmt::Display for DBsource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}, {}, {}, {}, {}, {}", self.jobid, self.submit, self.start, self.end, self.max_minutes, self.queue_minutes, self.run_minutes)
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
    pub run_minutes: i32,
    pub backlog_minutes: i32,
    pub backlog_num_jobs: i32,
    pub running_minutes: i32,
    pub running_num_jobs: i32,
}

impl DBtarget {
    fn new(src: DBsource, backlog_minutes: i32, backlog_num_jobs: i32, running_minutes: i32, running_num_jobs: i32) -> DBtarget {
        DBtarget {jobid: src.jobid, submit: src.submit, start: src.start, max_minutes: src.max_minutes, queue_minutes: src.queue_minutes,
                  run_minutes: src.run_minutes, backlog_minutes, backlog_num_jobs, running_minutes, running_num_jobs}
    }
}

impl fmt::Display for DBtarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}", self.jobid, self.submit, self.start, self.max_minutes, self.queue_minutes,
                            self.run_minutes, self.backlog_minutes, self.backlog_num_jobs, self.running_minutes, self.running_num_jobs)
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
    // Force parameter processing.
    CONFIG.println();
    if CONFIG.help {
        print_help();
        return;
    }

    // Connection information including password.
    let url = load_db_url();
    
    // Adjust query as needed.
    let select1 = "SELECT jobid, submit, start, end, max_minutes, \
                       TIMESTAMPDIFF(MINUTE, submit, start) as queue_minutes, \
                       TIMESTAMPDIFF(MINUTE, start, end) as run_minutes \
                       FROM :input_table \
                       WHERE queue = ':queue' AND state = 'COMPLETED' \
                       ORDER BY submit ASC";
    let select2 = select1.replacen(":input_table", &CONFIG.input_table, 1);
    let select  = select2.replacen(":queue", &CONFIG.queue, 1);

    // Connect to the database and panic if we can't.
    let pool = Pool::new(url.as_str()).expect("Failed to create pool.");
    let mut conn1 = pool.get_conn().expect("Failed to create conn1.");

    // Create the output table using the first connection.
    create_output_table(&mut conn1);

    // Create the queue with a capacity not likely to be exceeded.
    let mut backlog_queue: Vec<DBsource> = Vec::with_capacity(2000);
    let mut running_jobs:  Vec<DBsource> = Vec::with_capacity(2000);
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
                let r: (String, String, String, String, i32, i32, i32) = from_row::<(String, String, String, String, i32, i32, i32)>(a);
                let cur_job = DBsource::new(
                    r.0,
                    NaiveDateTime::parse_from_str(r.1.as_str(), "%Y-%m-%d %H:%M:%S").expect("Submit decode failure."),
                    NaiveDateTime::parse_from_str(r.2.as_str(), "%Y-%m-%d %H:%M:%S").expect("Start decode failure."),
                    NaiveDateTime::parse_from_str(r.3.as_str(), "%Y-%m-%d %H:%M:%S").expect("End decode failure."),
                    r.4,
                    r.5,
                    r.6,
                );

                // ------------------- Identify Queued Jobs -------------------
                // Set up for analyzing the current job's queue.
                let mut queue_keep: Vec<bool> = Vec::with_capacity(backlog_queue.len());
                let mut cur_backlog_num_jobs = 0;
                let mut cur_queue_minutes = 0;

                // Iterate through existing backlog queue looking for jobs whose start
                // time has not occurred when the current job was submitted.
                for backlog_job in &backlog_queue {
                    if backlog_job.start > cur_job.submit {
                        // Collect queue statistics.
                        cur_backlog_num_jobs += 1;
                        cur_queue_minutes += backlog_job.max_minutes;
                        queue_keep.push(true); // This element stays in queue.
                    } else {
                        // Promote backlog job to running job if
                        // its end time is still in the future.
                        if backlog_job.end > cur_job.submit {
                            running_jobs.push(backlog_job.clone());
                        }

                        // Mark current queue element for removal.
                        queue_keep.push(false);
                    }
                }

                // Delete the backlog jobs marked for removal.
                let mut iter1 = queue_keep.iter();
                backlog_queue.retain(|_| *iter1.next().expect("backlog.queue.retain failure"));

                // ------------------- Identify Running Jobs -------------------
                // Set up for analyzing the jobs currently running.
                let mut run_keep: Vec<bool> = Vec::with_capacity(running_jobs.len());
                let mut cur_running_num_jobs = 0;
                let mut cur_running_minutes = 0;

                // Iterate through existing running jobs list looking for jobs 
                // that have terminated by the time the current job was submitted.
                for running_job in &running_jobs {
                    if  running_job.end > cur_job.submit {
                            run_keep.push(false); // will be removed
                       } else {
                            // Collect running job statistics.
                            cur_running_num_jobs += 1;
                            cur_running_minutes += running_job.max_minutes;
                            run_keep.push(true);
                       }
                }

                // Delete the running jobs marked for removal.
                let mut iter2 = run_keep.iter();
                running_jobs.retain(|_| *iter2.next().expect("running.job.retain failure"));

                // ------------------- Assign Current Job ----------------------
                // We almost always push the current job onto the backlog queue,
                // but since the inputs might not be perfect we account for the
                // possibility that jobs immediately start running.
                if cur_job.start > cur_job.submit {
                    backlog_queue.push(cur_job.clone());
                } else {
                    running_jobs.push(cur_job.clone());
                }

                // ------------------- Save Current Job ------------------------
                // Create the current job's output record.
                let cur_job_output = DBtarget::new(cur_job, cur_queue_minutes, cur_backlog_num_jobs,
                                                             cur_running_minutes, cur_running_num_jobs);           
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
    let mut input_table = INVALID_TABLE.to_string();
    let mut output_table_suffix = OUTPUT_TABLE_SUFFIX.to_string();
    let mut queue: String = INVALID_QUEUE.to_string();
    let mut ignore_dups = false;
    let mut help: bool = false;

    // Force help when no arguments are given.
    if args.len() <= 1 {
        help = true;
    }

    // Check for help which is a standalone flag, no value.
    for key in &args {
        if key == "-help" || key == "--help" {
            help = true;
//            return Config::new(program_pathname, input_table, output_table_suffix, queue, ignore_dups, "".to_string(), help);   
        }
    }

    // Start with the first argument pair.
    let mut index = 1;
     while (index + 1) < args.len() {
        // Get the key and value.
        let key = &args[index];
        let val = &args[index+1];

        // See if they are known arguments.
        // --- Output Table
        if key == "-output_table_suffix" {
            output_table_suffix = val.clone();
        } 
        // --- Input Table
        else if key == "-input_table" {
            input_table = val.clone();
        }
        // --- Queue
        else if key == "-queue" {
            queue = val.clone();
        }
        // --- Ignore Duplicate Inserts
        else if key == "-ignore_dups" {
            if val == "true" {
                ignore_dups = true;
            }
        }
        else if key == "-help" || key == "--help" {
            // Help is handled separately above, but since
            // help doesn't take a value we have to only
            // increment the indext by 1.
            index += 1;
            continue;
        }
        // --- Abort on Unknown Parameter
        else {
            panic!("2");
        }

        // Increment to next pair.
        index += 2;
    }

    let output_table = make_output_table_name(&input_table, &output_table_suffix, &queue);
    Config {program_pathname, input_table, output_table_suffix, queue, ignore_dups, output_table, help}
}

// ---------------------------------------------------------------------------
// make_output_table_name:
// ---------------------------------------------------------------------------
/** Concatenate the output table name from user-supplied and constant strings.
 */
fn make_output_table_name(input_table: &String, output_table_suffix: &String, queue: &String) -> String {
    let mut tname = OUTPUT_TABLE_PREFIX.to_owned() + input_table + "_" + queue;
    if !output_table_suffix.is_empty() {
        tname = tname + "_" + output_table_suffix;
    }
    tname
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
                             run_minutes int unsigned NOT NULL, \
                             backlog_minutes int unsigned NOT NULL, \
                             backlog_num_jobs int unsigned NOT NULL, \
                             running_minutes int unsigned NOT NULL, \
                             running_num_jobs int unsigned NOT NULL)";
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
    conn.query_drop("CREATE INDEX index_run_minutes ON ".to_owned() + table_name + " (run_minutes)").expect("Create index 5 failure.");
    conn.query_drop("CREATE INDEX index_backlog_minutes ON ".to_owned() + table_name + " (backlog_minutes)").expect("Create index 6 failure.");
    conn.query_drop("CREATE INDEX index_backlog_num_jobs ON ".to_owned() + table_name + " (backlog_num_jobs)").expect("Create index 7 failure.");
    conn.query_drop("CREATE INDEX index_running_minutes ON ".to_owned() + table_name + " (running_minutes)").expect("Create index 8 failure.");
    conn.query_drop("CREATE INDEX index_running_num_jobs ON ".to_owned() + table_name + " (running_num_jobs)").expect("Create index 9 failure.");
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
            "run_minutes" => rec.run_minutes,
            "backlog_minutes" => rec.backlog_minutes,
            "backlog_num_jobs" => rec.backlog_num_jobs,
            "running_minutes" => rec.running_minutes,
            "running_num_jobs" => rec.running_num_jobs,
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
            (jobid, submit, start, max_minutes, queue_minutes, run_minutes, backlog_minutes, backlog_num_jobs, running_minutes, running_num_jobs) \
            VALUES (:jobid, :submit, :start, :max_minutes, :queue_minutes, :run_minutes, :backlog_minutes, :backlog_num_jobs, :running_minutes, :running_num_jobs)";
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

// ---------------------------------------------------------------------------
// print_help:
// ---------------------------------------------------------------------------
fn print_help() {
    // There's probably a better way to do this...
    let msg = "Smart Scheduling Time Series Analysis\n\n \
    This program takes as input the SQL table data produced by HPCDataLoad.py in the\n \
    smart-scheduling project (https://github.com/tapis-project/smart-scheduling)\n \
    and calculates for each job the current state of the queue and running programs\n \
    on the HPC system when the job was submitted.\n\n \
    The following parameters can be set:\n\n \
        \t-help - to print this message\n \
        \t-input_table - (REQUIRED) the source historical job data\n \
        \t-queue - (REQUIRED) the HPC queue or partition on which the jobs were scheduled\n \
        \t-output_table_suffix - a suffix appended to the output table name\n \
        \t-ignore_dups - ignore attempts to insert duplicate records into the output table\n\n \
    Results are written to a new output table with a name that conforms to the format:\n\n \
        \tjobq_<input_table>_<queue>[_output_table_suffix]\n\n \
    If a required parameter is not given, the program aborts.  For example, here are parameters\n \
    that specify processing all jobs from the normal queue of stampede2 and putting the results\n \
    in the jobq_stampede2_normal_test output table:\n\n \
        \tsmartsched_rust -input_table stampede2 -queue normal -output_table_suffix test\n";
    println!("{}", msg);
}

// ***************************************************************************
//                                 Tests
// ***************************************************************************
#[cfg(test)]
mod tests {
    use mysql::*;
    use mysql::prelude::*;
    use std::fs;

    // ------------------ In Scope Support Definitions
    // Hardcode where url/credentials file resides.
    const DB_URL_FILE: &str = "/<YOUR_PATH_GOES_HERE>/smartsched-db.url";
    fn load_db_url_for_test() -> String {
        fs::read_to_string(DB_URL_FILE).expect("Failed to load mysql url from file.")
    }
    // ------------------ End Support Definitions

    #[test]
    fn count_rows() {
        // **** SET EXECUTION PARAMETERS HERE ****
        let table = "stampede2";
        let clause = " WHERE queue = 'normal' AND state = 'COMPLETED'"; // adding state check slowed execution alot
        //let clause = "";

        // Connect to the database.
        let url = load_db_url_for_test();
        //let opts = Opts::from_url(url).unwrap(); 
        let pool = Pool::new(url.as_str()).unwrap();
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
