# Extract content from Parquet files for training ML models of LaTeX formulas

How to use:

1. Place the parquet file into `datasets/` folder.
2. Run the script with `cargo run`.
  a. For better performance you can build it first, with `cargo build --release` and then run with `./target/release/extract-parquet-to-fs`.

The outputs will be in `outputs/` folder, with subfolders with the same name as the parquet files. Each folder will have a `labels.json` file with the keys (name of the images) and values (LaTeX string value without spaces).
