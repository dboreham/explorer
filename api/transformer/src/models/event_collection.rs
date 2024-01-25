use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch};
use diem_api_types::Event;
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use std::{fs::File, sync::Arc};

pub struct EventCollection {
    version: Vec<u64>,
    creation_number: Vec<u64>,
    account_address: Vec<Vec<u8>>,
    sequence_number: Vec<u64>,
    module_address: Vec<Vec<u8>>,
    module_name: Vec<String>,
    struct_name: Vec<String>,
    data: Vec<String>,
    timestamp: Vec<u64>,
}

impl EventCollection {
    pub fn new() -> EventCollection {
        EventCollection {
            version: Vec::new(),
            creation_number: Vec::new(),
            account_address: Vec::new(),
            sequence_number: Vec::new(),
            module_address: Vec::new(),
            module_name: Vec::new(),
            struct_name: Vec::new(),
            data: Vec::new(),
            timestamp: Vec::new(),
        }
    }

    pub fn push(&mut self, version: u64, timestamp: u64, events: &Vec<Event>) {
        for event in events {
            self.version.push(version);
            self.timestamp.push(timestamp);
            self.creation_number.push(event.guid.creation_number.into());
            self.account_address
                .push(event.guid.account_address.inner().to_vec());
            self.sequence_number.push(event.sequence_number.into());
            self.data.push(serde_json::to_string(&event.data).unwrap());

            match &event.typ {
                diem_api_types::MoveType::Struct(s) => {
                    self.module_address.push(s.address.inner().to_vec());
                    self.module_name.push(s.module.0.to_string());
                    self.struct_name.push(s.name.0.to_string());
                }
                move_type => {
                    panic!("Invalid event move type {:?}", move_type);
                }
            }
        }
    }

    pub fn to_parquet(&self, path: String) {
        if self.version.is_empty() {
            return;
        }

        let version = arrow_array::UInt64Array::from(self.version.clone());
        let timestamp = arrow_array::UInt64Array::from(self.timestamp.clone());
        let creation_number = arrow_array::UInt64Array::from(self.creation_number.clone());
        let account_address =
            FixedSizeBinaryArray::try_from_iter(self.account_address.iter()).unwrap();
        let sequence_number = arrow_array::UInt64Array::from(self.sequence_number.clone());
        let module_address =
            FixedSizeBinaryArray::try_from_iter(self.module_address.iter()).unwrap();

        let module_name = arrow_array::StringArray::from(self.module_name.clone());
        let struct_name = arrow_array::StringArray::from(self.struct_name.clone());
        let data = arrow_array::StringArray::from(self.data.clone());

        let batch = RecordBatch::try_from_iter(vec![
            ("version", Arc::new(version) as ArrayRef),
            ("timestamp", Arc::new(timestamp) as ArrayRef),
            ("creation_number", Arc::new(creation_number) as ArrayRef),
            ("account_address", Arc::new(account_address) as ArrayRef),
            ("sequence_number", Arc::new(sequence_number) as ArrayRef),
            ("module_address", Arc::new(module_address) as ArrayRef),
            ("module_name", Arc::new(module_name) as ArrayRef),
            ("struct_name", Arc::new(struct_name) as ArrayRef),
            ("data", Arc::new(data) as ArrayRef),
        ])
        .unwrap();

        let parquet_file = File::create(path).unwrap();
        let props = WriterProperties::builder().build();

        let mut writer = ArrowWriter::try_new(parquet_file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
    }
}