# Car Crash Case Study

This repository contains a case study on U.S. vehicle accident analysis using PySpark. The project involves data processing, transformation, and analysis of car crash datasets, with the results saved in the desired format. It is designed to run on Google Colab and is structured to ensure modularity and ease of use.

---

## Project Structure

```
car_crash_case_study/
├── main.ipynb             # Jupyter Notebook for running the analysis
├── src/
│   ├── us_accident_analysis_package.py   # Python package for accident analysis
│   └── __init__.py        # Package initializer
├── input/
│   └── *.csv             # Input CSV files required for the transformations
├── output/
│   └── <results>         # Folder for output results
└── config.yaml             # Configuration file containing input/output paths and file formats
```

---

## Setup Instructions

### Prerequisites

1. **Google Colab**
   Ensure you have access to Google Colab.

2. **Google Drive**
   Store the project folder (`car_crash_case_study`) in `My Drive`.


### Folder Structure in Google Drive
Place the `car_crash_case_study` folder in your Google Drive under the following path:
```
My Drive/
├── car_crash_case_study/
```

---

## Configuration

The `config.yaml` file contains all the necessary details for input and output paths, along with file format specifications. 

---

## How to Run

1. **Mount Google Drive**
   Run the following command in the `main.ipynb` notebook to mount your Google Drive:
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```

2. **Run the Analysis**
   Open and execute the `main.ipynb` notebook to load the data, process transformations, and generate output.

---

## Features

- **Modular Codebase**: All transformation logic is encapsulated in the `src/us_accident_analysis_package.py` package.
- **Dynamic Configuration**: Easily manage input/output paths and file formats using `config.yaml`.
- **Output Results**: Save analyzed data to the `output` folder in the desired format (default: CSV).

---

## File Descriptions

### `main.ipynb`
The entry point for running the analysis. It:
- Loads configuration details from `config.yaml`.
- Executes data transformations using the `us_accident_analysis_package.py` package.

### `us_accident_analysis_package.py`
Contains the following functionalities:
- Load CSV files into DataFrames.
- Perform various transformations and analyses, including:
  - Counting male crashes.
  - Identifying two-wheeler crashes.
  - Determining top vehicle makes for fatal crashes.
  - And more.

### `config.yaml`
Specifies the paths for input CSV files, output folder, and file format.

---

## Results

Processed results are stored in the `output` folder. Each transformation creates a corresponding output file.

---

## Author

Adithya V Amberker

[LinkedIn Profile](https://www.linkedin.com/in/adithya-v-amberker-0b12b828b/)

