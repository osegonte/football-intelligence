from setuptools import setup, find_packages

setup(
    name="football-intelligence",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        # Core dependencies
        "pandas>=2.1.0",
        "numpy>=1.26.0",
        
        # Web scraping
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.2",
        "cloudscraper>=1.2.71",
        "selenium>=4.14.0",
        "webdriver-manager>=4.0.1",
        "lxml>=4.9.3",
        "html5lib>=1.1",
        
        # Data processing and analysis
        "scikit-learn>=1.3.1",
        "scipy>=1.11.3",
        "statsmodels>=0.14.0",
        
        # Visualization
        "matplotlib>=3.8.0",
        "seaborn>=0.13.0",
        "plotly>=5.17.0",
        
        # Dashboard
        "streamlit>=1.27.2",
        "altair>=5.1.1",
        
        # Utilities
        "python-dateutil>=2.8.2",
        "pytz>=2023.3.post1",
        "tqdm>=4.66.1",
        "pyyaml>=6.0.1",
        "joblib>=1.3.2",
    ],
    entry_points={
        "console_scripts": [
            "football-scraper=main:main",
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="Football match intelligence and analytics platform",
    keywords="football, soccer, scraping, analytics, dashboard",
    python_requires=">=3.8",
)

# Update setup.py install_requires list
install_requires=[
    # Existing dependencies...
    
    # Database dependencies
    "psycopg2-binary>=2.9.5",
    "sqlalchemy>=2.0.0",
    "alembic>=1.10.0",
]