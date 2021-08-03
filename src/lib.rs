#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate tempdir;

pub mod log;

#[cfg(test)]
mod test {
    use crate::log::Coordinator;
    use super::log::{Log,Record,Consumer};
    use tempdir::TempDir;

    #[test]
    fn basic_write_and_read(){
        let dir = TempDir::new_in(".","logs").expect("creating tempdir");
        let mut  log = Log::new(&dir).expect("failed to build log");
        let mut consumer = Consumer::new(&dir).expect("failed to build consumer");
        let batch_in = vec![

            Record::new("first message"),
            Record::new("second message"),
        ];
       log.append(&batch_in).expect("failed to send batch");
       let batch_out =  consumer.poll().expect("failed to poll batch");
       assert_eq!(batch_in,batch_out);        
    }

    #[test]
    fn consumer_starts_from_the_end(){
        let dir = TempDir::new_in(".","logs").expect("creating tempdir");
        let mut  log = Log::new(&dir).expect("failed to build log");

        let first_batch = vec![
            Record::new("i am the first message"),
            Record::new("i am the second message"),
        ];
        log.append(&first_batch).expect("failed to send batch");

        let mut consumer = Consumer::new(&dir).expect("failed to build consumer");

        let second_batch = vec![
            Record::new("i am the third message"),
            Record::new("i am the fourth message"),
        ];
        log.append(&second_batch).expect("failed to send batch");

        let batch_out = consumer.poll().expect("failed to poll for batch");
        assert_eq!(second_batch, batch_out);

    }

    #[test]
    fn logs_split_into_segments() {
        let dir = TempDir::new_in(".","logs").expect("creating tempdir");
        let mut log = Log::new(&dir).expect("failed to build log");
        let mut consumer = Consumer::new(&dir).expect("failed to build consumer");

        let records = vec![
            Record::new("i am the first message"),
            Record::new("i am the second message")
        ];

        log.append(&records[..1]).expect("failed to append first message");

        log.roll_segment().expect("failed to roll segment");

        log.append(&records[1..]).expect("failed to append second");

        assert_eq!(2, std::fs::read_dir(&dir).unwrap().count());
        assert_eq!(records, consumer.poll().expect("failed to poll"));


    }

    
    #[test]
    fn only_retains_segments_with_active_consumers() {
        let dir = TempDir::new_in(".","logs").expect("creating tempdir");
        let mut coordinator = Coordinator::default();
        let mut log =coordinator.create_log(&dir).expect("failed to build log");
        let mut consumer = Consumer::new(&dir).expect("failed to build consumer");
        
        let records = vec![
            Record::new("i am the first message"),
            Record::new("i am the second message")
        ];

        log.append(&records[..1]).expect("failed to append first message");

        log.roll_segment().expect("failed to roll segment");

        log.append(&records[1..]).expect("failed to append second");

        assert_eq!(2, std::fs::read_dir(&dir).unwrap().count());
        assert_eq!(records, consumer.poll().expect("failed to poll"));
        consumer.commit_offsets(&mut coordinator);

        // make this auto
        coordinator.enforce_retention().expect("failed to enforce retention");
        assert_eq!(1, ::std::fs::read_dir(&dir).unwrap().count());

    }
}



