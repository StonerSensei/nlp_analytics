# Hospital Analytics - Natural Language to SQL Query System

A production-ready system that converts natural language questions into SQL queries using Ollama and SQLCoder, with automatic CSV schema detection and data loading.

## Overview

This system allows users to upload CSV files, automatically detect database schemas, and query data using natural language. It leverages large language models (LLMs) for SQL generation, providing an intuitive interface for data analysis without requiring SQL knowledge.

## Features

### Core Functionality
- Natural Language to SQL conversion using Ollama with SQLCoder 7B model
- Automatic CSV schema detection and table creation
- Data type inference (INTEGER, VARCHAR, FLOAT, DATE, BOOLEAN)
- Primary and foreign key identification
- Bulk data loading with validation
- RESTful API for all operations

### User Interface
- Streamlit-based web interface
- CSV file upload with preview
- Natural language query input
- Interactive results display
- Data export functionality
- Real-time system health monitoring

### Analytics
- Metabase integration for advanced visualizations
- Interactive dashboards
- Custom report generation
- Scheduled analytics

## Technology Stack

### Backend
- Python 3.11
- FastAPI - Modern web framework for APIs
- SQLAlchemy 2.0 - SQL toolkit and ORM
- Pandas - Data manipulation and analysis
- Pydantic - Data validation

### Frontend
- Streamlit - Interactive web applications
- Requests - HTTP library

### Database
- PostgreSQL 15 - Relational database
- psycopg2 - PostgreSQL adapter

### AI/ML
- Ollama - Local LLM inference
- SQLCoder 7B - Specialized SQL generation model

### Analytics
- Metabase - Business intelligence and analytics

### Infrastructure
- Docker - Containerization
- Docker Compose - Multi-container orchestration

## Architecture

                      Docker Network
┌────────────────────────────────────────────────────┐
│                                                     │
│  ┌──────────────┐       ┌──────────────┐          │
│  │   Streamlit  │◄──────│   FastAPI    │          │
│  │   Frontend   │       │   Backend    │          │
│  │  Port: 8501  │       │  Port: 8000  │          │
│  └──────────────┘       └───────┬──────┘          │
│                                  │                  │
│         ┌────────────────────────┼──────────────┐  │
│         │                        │              │  │
│    ┌────▼─────┐          ┌──────▼────┐   ┌────▼────┐
│    │PostgreSQL│          │  Ollama   │   │Metabase │
│    │          │          │ SQLCoder  │   │         │
│    │Port: 5432│          │Port:11434 │   │Port:3000│
│    └──────────┘          └───────────┘   └─────────┘
└────────────────────────────────────────────────────┘



## Prerequisites

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- Minimum 8GB RAM
- 20GB free disk space (for Docker images and AI models)

## Installation

### 1. Clone the Repository

- https://github.com/StonerSensei/nlp_analytics
- cd nlp_analytics

### 2. Configure Environment

- Create a `.env` file in the project root:


### 3. Start All Services

- docker compose up -d


