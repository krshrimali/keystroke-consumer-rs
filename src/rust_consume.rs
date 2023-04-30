use kafka::{
    consumer::{Consumer, GroupOffsetStorage},
    error::Error as KafkaError,
};

pub fn receive_msg() {
    let broker = "localhost:9092".to_owned();
    let topic = "test".to_owned();

    if let Err(e) = consume_messages(topic, vec![broker]) {
        println!("Failed consuming messages: {:?}", e);
    }
}

fn consume_messages(topic: String, brokers: Vec<String>) -> Result<(), KafkaError> {
    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        // .with_group(group)
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        // .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;

    loop {
        let mss = con.poll()?;
        // if mss.is_empty() {
        //     println!("No messages available right now.");
        //     // return Ok(());
        // }

        for ms in mss.iter() {
            for m in ms.messages() {
                println!(
                    "{}:{}@{}: {:?}",
                    ms.topic(),
                    ms.partition(),
                    m.offset,
                    String::from_utf8(m.value.to_owned()).unwrap()
                );
            }
            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed()?;
    }
}
