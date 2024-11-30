use math_images_processor::ImageProcessorConfig;
use parquet::data_type::AsBytes;
use parquet::data_type::ByteArray;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use parquet::record::{Row, RowAccessor};
use std::collections::HashMap;
use std::fs::{self, File, ReadDir};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Path to your Parquet file
    let datasets = "datasets";

    // Directory to save the images
    let output_dir = "outputs";

    // Create the output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;

    // Create a corresponding output folder for every dataset
    let entries: ReadDir = fs::read_dir(datasets)?;
    for entry in entries {
        let path = entry?.path();

        // Check if the file is a .parquet file
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            // Extract the filename without the .parquet extension
            if let Some(file_name) = path.file_stem().and_then(|s| s.to_str()) {
                // Create the corresponding output subfolder
                let subfolder = format!("{}/{}", output_dir, file_name);
                fs::create_dir_all(&subfolder)?;

                // Create a HashMap to store labels for the JSON file
                let mut labels: HashMap<String, String> = HashMap::new();

                // Open the Parquet file
                let file: File = File::open(path)?;
                let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
                let mut iter: RowIter<'_> = reader.get_row_iter(None)?;

                // Iterate over rows in the Parquet file
                while let Some(row) = iter.next() {
                    let success_row: Row = row.unwrap();
                    let image_data: &Row = success_row.get_group(0).unwrap();
                    let latex_data: &String = success_row.get_string(1).unwrap();
                    let image_bytes: &ByteArray = image_data.get_bytes(0).unwrap();
                    let file_name: &String = image_data.get_string(1).unwrap();

                    let img = image::load_from_memory(image_bytes.as_bytes()).unwrap();
                    let processed_image = math_images_processor::preprocess_image(
                        img,
                        &ImageProcessorConfig::default(),
                    )?;

                    // Save the image data to a file in the subfolder
                    let image_file_path = format!("{}/{}", subfolder, file_name);
                    processed_image.save(image_file_path)?;

                    // Remove spaces from latex_data and save it in the labels map
                    let label = latex_data.replace(" ", "");
                    labels.insert(file_name.clone(), label);
                }

                // Save the labels map to labels.json in the same subfolder
                let labels_file_path = format!("{}/labels.json", subfolder);
                let labels_file = File::create(labels_file_path)?;
                serde_json::to_writer_pretty(labels_file, &labels)?;
            }
        }
    }

    Ok(())
}
