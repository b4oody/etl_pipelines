import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """
    Абстрактный базовый класс для loader'ів
    Поддерживает разные типы БД
    """

    def __init__(self, table_name: str, logger_instance=None):
        self.table_name = table_name
        self.logger = logger_instance or logger

    @abstractmethod
    def load(self, df, mode: str = "upsert") -> Dict[str, Any]:
        """
        Load data into database
        mode: 'insert', 'upsert', 'replace'
        """
        pass

    @abstractmethod
    def table_exists(self) -> bool:
        """Check if table exists in database"""
        pass

    @abstractmethod
    def close(self):
        """Close database connection"""
        pass
