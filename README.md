# DBLP Semantic Scholar Data Processing Pipeline

A comprehensive academic paper data processing system that enriches DBLP paper data with Semantic Scholar metadata, performs author disambiguation, and provides automated scheduling capabilities.

## 🎯 Project Overview

This project integrates multiple academic databases to create a unified and enriched dataset of computer science papers, particularly focusing on Natural Language Processing conferences like ACL, NAACL, EMNLP, and others.

### Core Objectives
- **Data Integration**: Combine DBLP bibliographic data with Semantic Scholar's semantic enrichment
- **Author Analysis**: Create comprehensive author profiles with disambiguation and metrics calculation
- **PDF Management**: Download and organize academic papers in PDF format
- **Automated Processing**: Schedule and manage long-running data processing pipelines
- **Quality Validation**: Ensure data accuracy through multi-tier validation systems

### Key Features
- 🔄 **Incremental Processing**: Resume interrupted operations seamlessly
- 📊 **Rich Analytics**: Generate detailed reports and statistics
- 🤖 **Smart Scheduling**: Automated 7-day processing cycles with customizable schedules
- 🎯 **Multi-tier Matching**: Advanced paper matching using DOI, ACL IDs, and semantic similarity
- 📚 **PDF Integration**: Automatic PDF download with metadata linking
- 🔍 **Author Disambiguation**: Sophisticated author identity resolution

## 🏗️ System Architecture

The system is organized into multiple processing phases:

### Phase 1: Data Acquisition & Integration
1. **DBLP Import**: Download and parse DBLP XML data
2. **S2 Enrichment**: Match and enrich papers with Semantic Scholar data
3. **PDF Collection**: Download available PDFs with metadata tracking

### Phase 2: Author Processing
1. **Authorship Extraction**: Create paper-author relationship tables
2. **Author Profiles**: Build comprehensive author profiles
3. **Metrics Calculation**: Compute advanced author metrics and statistics
4. **Final Integration**: Create unified author tables with all metadata

### Phase 3: Automation & Scheduling
1. **Comprehensive Pipeline**: Integrated processing of all phases
2. **Scheduled Operations**: Automated 7-day processing cycles
3. **Progress Monitoring**: Real-time status tracking and logging

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- PostgreSQL database
- UV package manager
- Semantic Scholar API key (optional but recommended)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd semantic
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Setup environment variables**
   Create a `.env` file:
   ```bash
   # Database Configuration
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=semantic_db
   DB_USER=your_user
   DB_PASSWORD=your_password
   
   # Semantic Scholar API (optional)
   SEMANTIC_SCHOLAR_API_KEY=your_api_key
   ```

4. **Initialize database**
   ```bash
   uv run python scripts/setup_database.py
   ```

## 📋 Usage Guide

### Quick Start: Automated Processing

For complete hands-off processing with scheduling:

```bash
# Start weekly automated processing (every 7 days at 2 AM)
uv run python scripts/run_comprehensive_scheduler.py

# Run immediately (manual mode)
uv run python scripts/run_comprehensive_scheduler.py --manual

# Custom schedule (every 3 days at 2 AM)
uv run python scripts/run_comprehensive_scheduler.py --custom "0 2 */3 * *"

# List scheduled jobs
uv run python scripts/run_comprehensive_scheduler.py --list
```

### Step-by-Step Processing

#### Phase 1: Core Data Processing

1. **DBLP Data Import**
   ```bash
   uv run python scripts/run_dblp_service_once.py
   ```
   Downloads and parses DBLP XML, extracts papers from target conferences

2. **Semantic Scholar Enrichment**
   ```bash
   uv run python scripts/run_s2_enrichment.py
   ```
   Matches DBLP papers with Semantic Scholar, adds semantic metadata

3. **PDF Download**
   ```bash
   uv run python scripts/run_pdf_download.py
   ```
   Downloads available PDFs and links them to paper records

#### Phase 2: Author Processing

Run all author processing steps:
```bash
uv run python scripts/run_all_steps.py
```

Or run individual steps:

1. **Create Authorships**
   ```bash
   uv run python scripts/step1_create_authorships.py
   ```

2. **Build Author Profiles**
   ```bash
   uv run python scripts/step2_create_author_profiles.py
   ```

3. **Calculate Metrics**
   ```bash
   uv run python scripts/step3_calculate_metrics.py
   ```

4. **Create Final Tables**
   ```bash
   uv run python scripts/step4_create_final_table.py
   ```

5. **Generate Reports**
   ```bash
   uv run python scripts/step5_generate_reports.py
   ```

## 📊 Data Processing Flow

### 1. DBLP Data Acquisition
- Downloads compressed XML from DBLP (~3GB)
- Parses XML to extract paper metadata
- Filters for target conferences (ACL, NAACL, EMNLP, etc.)
- Stores in `dblp_papers` table

### 2. Semantic Scholar Integration
- Matches DBLP papers using multiple strategies:
  - **Tier 1**: DOI and ACL ID exact matching
  - **Tier 2**: Title and author similarity matching
  - **Tier 3**: Fuzzy matching with confidence scoring
- Enriches with:
  - Abstract text
  - Citation counts
  - Author affiliations
  - Research field classifications
  - Open access PDF links

### 3. PDF Management
- Downloads PDFs from open access sources
- Organizes files with consistent naming
- Links PDFs to paper records in database
- Tracks download status and file metadata

### 4. Author Disambiguation
- Creates `authorships` table linking papers to authors
- Builds `author_profiles` with aggregated information
- Calculates advanced metrics:
  - Publication counts by venue and year
  - Citation statistics
  - Collaboration networks
  - Research evolution tracking

## 🗂️ Project Structure

```
semantic/
├── scripts/                    # Main execution scripts
│   ├── run_comprehensive_scheduler.py  # Automated scheduling
│   ├── run_dblp_service_once.py       # DBLP data import
│   ├── run_s2_enrichment.py           # S2 enrichment
│   ├── run_pdf_download.py            # PDF collection
│   ├── run_all_steps.py               # Author processing
│   ├── step1_create_authorships.py    # Authorship extraction
│   ├── step2_create_author_profiles.py # Author profiles
│   ├── step3_calculate_metrics.py     # Metrics calculation
│   ├── step4_create_final_table.py    # Final integration
│   ├── step5_generate_reports.py      # Report generation
│   └── setup_database.py              # Database initialization
├── src/semantic/               # Core library code
│   ├── database/              # Database connections and schemas
│   ├── services/              # Business logic services
│   ├── scheduler/             # Automated scheduling system
│   └── utils/                 # Utilities and configuration
├── data/                      # Data storage directory
│   ├── pdfs/                 # Downloaded PDF files
│   └── *.csv                 # Export files
├── logs/                     # Application logs
├── external/                 # External data (DBLP XML)
└── docs/                    # Additional documentation
```

## 📈 Monitoring & Analytics

### Real-time Progress Tracking
- Processing speeds and ETAs
- Success/failure rates by processing tier
- Database growth statistics
- PDF download completion rates

### Generated Reports
- **Enrichment Statistics**: Coverage rates, match quality distribution
- **Author Analytics**: Publication patterns, collaboration networks
- **Data Quality Reports**: Validation results, error summaries
- **Processing Performance**: Timing analysis, bottleneck identification

### Export Formats
- CSV exports for external analysis
- JSON validation reports
- Detailed processing logs

## ⚙️ Configuration

### Environment Variables
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=semantic_db
DB_USER=postgres
DB_PASSWORD=password

# API Keys
SEMANTIC_SCHOLAR_API_KEY=your_key_here

# Processing
S2_RATE_LIMIT_PER_SECOND=100
PDF_DOWNLOAD_MAX_CONCURRENT=5
BATCH_SIZE=1000
```

### Target Conferences
Configure in `src/semantic/utils/config.py`:
```python
TARGET_VENUES = {
    'acl', 'naacl', 'emnlp', 'findings',
    'eacl', 'aacl', 'coling', 'lrec'
}
```

## 🔧 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify PostgreSQL is running
   - Check credentials in `.env` file
   - Ensure database exists

2. **API Rate Limits**
   - Get Semantic Scholar API key for higher limits
   - Adjust rate limiting in configuration
   - Use manual mode for testing

3. **Memory Issues with Large Datasets**
   - Reduce batch sizes in configuration
   - Process in smaller chunks
   - Monitor available RAM

4. **PDF Download Failures**
   - Check network connectivity
   - Verify SSL certificates
   - Review blocked domains

### Logging
Detailed logs are available in the `logs/` directory:
- `comprehensive_pipeline_YYYYMMDD.log` - Main pipeline logs
- `s2_enrichment.log` - Semantic Scholar processing
- `step[1-5]_*.log` - Individual step logs

## 🤝 Contributing

1. Follow existing code style and patterns
2. Add comprehensive logging for new features
3. Include error handling and recovery mechanisms
4. Update documentation for any new functionality
5. Test with small datasets before full processing runs

## 📝 License

[License information to be added]

## 🙋 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review logs in the `logs/` directory
3. Consult the detailed documentation in `docs/`
4. File issues on the project repository

---

**Note**: This system is designed for academic research purposes. Ensure compliance with data usage policies of DBLP and Semantic Scholar when using this software.