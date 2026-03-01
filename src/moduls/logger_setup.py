import logging
import logging.handlers
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional


class LoggerSetup:
    """
    Centralized logging configuration for the application.
    Handles file and console logging with stage-based and custom log levels.
    """
    
    STAGE_LOG_LEVELS = {
        "dev": logging.DEBUG,
        "test": logging.DEBUG,
        "prod": logging.INFO,
    }
    
    DEFAULT_LOG_DIR = Path(__file__).parent.parent.parent / "logs"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    def __init__(
        self,
        stage: str = "prod",
        log_level: Optional[int] = None,
        log_file: Optional[str] = None,
    ):
        """
        Initialize logger setup.
        
        Args:
            stage: Execution stage (dev, test, prod)
            log_level: Custom log level (logging.DEBUG, logging.INFO, etc.)
                      If None, uses stage-based default
            log_file: Custom log file path. If None, uses default under logs/
        """
        self.stage = stage
        self.log_level = log_level or self.STAGE_LOG_LEVELS.get(stage, logging.WARNING)
        self.log_file = log_file
        self.logger = None
    
    def _get_log_file_path(self) -> Path:
        """
        Generate log file path with timestamp.
        
        Returns:
            Path to log file
        """
        if self.log_file:
            return Path(self.log_file)
        
        # Create logs directory if it doesn't exist
        self.DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}.log"
        return self.DEFAULT_LOG_DIR / filename
    
    def _create_console_handler(self) -> logging.StreamHandler:
        """
        Create console handler with formatter.
        
        Returns:
            Configured StreamHandler
        """
        handler = logging.StreamHandler()
        handler.setLevel(self.log_level)
        formatter = logging.Formatter(self.LOG_FORMAT, datefmt=self.DATE_FORMAT)
        handler.setFormatter(formatter)
        return handler
    
    def _create_file_handler(self, log_file_path: Path) -> logging.FileHandler:
        """
        Create file handler with formatter.
        
        Args:
            log_file_path: Path to log file
            
        Returns:
            Configured FileHandler
        """
        handler = logging.FileHandler(log_file_path, encoding="utf-8")
        handler.setLevel(self.log_level)
        formatter = logging.Formatter(self.LOG_FORMAT, datefmt=self.DATE_FORMAT)
        handler.setFormatter(formatter)
        return handler
    
    def _cleanup_old_logs(self, max_age_days: int = 30) -> None:
        """
        Delete log files older than specified days.
        
        Args:
            max_age_days: Maximum age of log files in days (default: 30)
        """
        if not self.DEFAULT_LOG_DIR.exists():
            return
        
        try:
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
            deleted_count = 0
            deleted_files = []
            
            # Find all .log files in the log directory
            for log_file in self.DEFAULT_LOG_DIR.glob("*.log"):
                try:
                    # Extract timestamp from filename (format: YYYYMMDD_HHMMSS.log)
                    filename = log_file.stem  # filename without extension
                    
                    # Try to parse the date from the filename
                    # Expected format: YYYYMMDD_HHMMSS
                    if len(filename) >= 8:
                        date_str = filename[:8]  # Extract YYYYMMDD
                        file_date = datetime.strptime(date_str, "%Y%m%d")
                        
                        # Check if file is older than cutoff date
                        if file_date < cutoff_date:
                            log_file.unlink()
                            deleted_count += 1
                            deleted_files.append(log_file.name)
                            
                except (ValueError, OSError) as e:
                    # Skip files that don't match the expected format or can't be deleted
                    continue
            
            if deleted_count > 0:
                self.logger.info(f"Cleaned up {deleted_count} old log file(s) older than {max_age_days} days")
                self.logger.debug(f"Deleted files: {', '.join(deleted_files)}")
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Error during log cleanup: {e}")
    
    def setup(self, name: str = "app") -> logging.Logger:
        """
        Setup and return configured logger.
        
        Args:
            name: Logger name (usually __name__)
            
        Returns:
            Configured logger instance
        """
        root_logger = logging.getLogger()
        root_logger.setLevel(self.log_level)

        # Remove existing handlers to avoid duplicates
        root_logger.handlers.clear()

        # Add console handler
        console_handler = self._create_console_handler()
        root_logger.addHandler(console_handler)

        # Add file handler
        log_file_path = self._get_log_file_path()
        file_handler = self._create_file_handler(log_file_path)
        root_logger.addHandler(file_handler)

        # Return a named logger for caller usage (records still go via root handlers)
        self.logger = logging.getLogger(name)

        # Log initial message
        self.logger.debug(
            f"Logger initialized - Stage: {self.stage}, Level: {logging.getLevelName(self.log_level)}"
        )
        
        # Cleanup old log files after logger is configured
        self._cleanup_old_logs()

        return self.logger


def get_logger(
    stage: str = "prod",
    log_level: Optional[int] = None,
    log_file: Optional[str] = None,
    name: str = "app",
) -> logging.Logger:
    """
    Convenience function to setup and get logger in one call.
    
    Args:
        stage: Execution stage (dev, test, prod)
        log_level: Custom log level (logging.DEBUG, logging.INFO, etc.)
        log_file: Custom log file path
        name: Logger name
        
    Returns:
        Configured logger instance
        
    Example:
        logger = get_logger(stage="dev", log_file="/tmp/custom.log")
        logger.info("Application started")
    """
    logger_setup = LoggerSetup(stage=stage, log_level=log_level, log_file=log_file)
    return logger_setup.setup(name=name)
