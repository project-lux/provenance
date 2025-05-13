# Provenance

An approach to parsing natural language provenance and outputting structured data.

## Overview

This project provides tools and utilities for parsing natural language descriptions of provenance (the history or origin of something) and converting them into structured data formats.

## Codebase Structure

The project is organized as follows:

```
.
├── main.py              # Main processing script
├── structure.yaml       # Data model and rules
├── index.html          # Web interface
├── output/             # Successful processing results
├── output_failed/      # Failed processing results
├── scripts/            # Utility scripts
├── notebooks/          # Jupyter notebooks
└── src/               # Source code
```

### Key Components

1. **Main Processing Script (`main.py`)**
   - Uses Google's Gemini AI models to parse provenance text
   - Converts natural language descriptions into structured JSON data
   - Handles parallel processing of multiple provenance entries
   - Includes error handling and retry logic
   - Outputs results to JSON files

2. **Data Model (`structure.yaml`)**
   - Defines the data model and rules for provenance parsing
   - Includes different types of provenance events (sales, gifts, inheritance, etc.)
   - Specifies rules for formatting dates, locations, and relationships
   - Provides examples of properly formatted provenance entries

3. **Data Structure**
   The system uses Pydantic models to structure the data:
   - `Actor`: Represents people, institutions, or other entities
   - `Movement`: Represents a change in ownership/location
   - `Provenance`: A collection of movements that make up the complete history

4. **Processing Flow**
   1. Reads provenance data from a CSV file
   2. Processes each entry using AI models
   3. Validates the output against the defined schema
   4. Saves successful results to the `output/` directory
   5. Saves failed attempts to `output_failed/` directory

## Getting Started

### Prerequisites

- Python 3.x
- Modern web browser

### Running the Web Interface

To run the web interface locally:

1. Navigate to the project directory in your terminal
2. Run the following command to start a local server:
   ```bash
   python -m http.server 8000
   ```
3. Open your web browser and visit:
   ```
   http://localhost:8000
   ```

The server will serve the `index.html` file and any other static assets in the directory.

## Features

- Natural language processing of provenance descriptions
- Conversion to structured data formats
- Web-based interface for easy interaction
- Parallel processing of multiple provenance entries
- Multiple AI model support (currently using Gemini 2.5)
- Robust error handling and retry logic
- Structured output in JSON format

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
