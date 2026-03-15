# src/domain/exceptions.py


class ETLError(Exception):
    """ETL処理におけるベースとなるカスタム例外"""

    pass


class APIRateLimitError(ETLError):
    """APIのレート制限（429 Too Many Requests）が上限に達した場合のエラー"""

    pass


class DatabaseError(ETLError):
    """データベースの操作・トランザクションに失敗した場合のエラー"""

    pass
