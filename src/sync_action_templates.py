class SyncActionTemplates:
    @staticmethod
    def success(message: str) -> dict:
        return {
            "status": "success",
            "type": "info",
            "message": message,
        }

    @staticmethod
    def error(message: str) -> dict:
        return {
            "status": "danger",
            "type": "info",
            "message": message,
        }
