use std::sync::mpsc::Receiver;

pub fn get_partitions() -> Vec<String> {
    vec!["hello".to_string(), "world".to_string()]
}

pub fn mine_partition(seed_receiver_channel: Receiver<String>) {
    
}