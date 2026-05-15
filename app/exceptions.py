class CheeseBoardException(Exception):
    def __init__(self, code: str, message: str, status_code: int = 500):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ChannelNotFoundException(CheeseBoardException):
    def __init__(self, channel_id: str):
        super().__init__("CHANNEL_NOT_FOUND", f"채널을 찾을 수 없습니다: {channel_id}", 404)


class CrawlJobNotFoundException(CheeseBoardException):
    def __init__(self, job_id: str):
        super().__init__("JOB_NOT_FOUND", f"크롤 작업을 찾을 수 없습니다: {job_id}", 404)


class StreamerNotFoundException(CheeseBoardException):
    def __init__(self, channel_id: str):
        super().__init__(
            "STREAMER_NOT_FOUND", f"등록된 스트리머를 찾을 수 없습니다: {channel_id}", 404
        )


class VideoNotFoundException(CheeseBoardException):
    def __init__(self, video_no: int):
        super().__init__("VIDEO_NOT_FOUND", f"영상을 찾을 수 없습니다: {video_no}", 404)


class ClipNotFoundException(CheeseBoardException):
    def __init__(self, clip_uid: str):
        super().__init__("CLIP_NOT_FOUND", f"클립을 찾을 수 없습니다: {clip_uid}", 404)


class ChzzkAPIException(CheeseBoardException):
    def __init__(self, message: str = "CHZZK API 호출에 실패했습니다."):
        super().__init__("CHZZK_API_ERROR", message, 502)


class InvalidRequestException(CheeseBoardException):
    def __init__(self, message: str):
        super().__init__("INVALID_REQUEST", message, 400)
