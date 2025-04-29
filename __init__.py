_version__ = '0.1.0'
__author__ = 'osegonte'

# Make important modules available at package level
from data_processing.analyzer import FootballDataAnalyzer
from data_processing.cleaner import FootballDataCleaner
from data_processing.predictor import MatchPredictor
from scrapers.sofascore_scraper import AdvancedSofaScoreScraper
from scrapers.fbref_scraper import FBrefScraper

# Define what's available when using "from football_intelligence import *"
__all__ = [
    'FootballDataAnalyzer',
    'FootballDataCleaner',
    'MatchPredictor',
    'AdvancedSofaScoreScraper',
    'FBrefScraper'
]