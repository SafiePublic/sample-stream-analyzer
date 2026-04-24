import concurrent.futures
import datetime
import io
import json
import logging
import random
from dataclasses import dataclass, field
from typing import Iterator

import av.error
import av.video.frame
import click
import grpc
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from PIL import Image

from proto.stream.v1.analyzer_pb2 import (
    DeviceStatus,
    EventPicture,
    ObjectPicture,
    StreamAnalyzeRequest,
    StreamAnalyzeResponse,
)
from proto.stream.v1.analyzer_pb2_grpc import (
    StreamAnalyzerServiceServicer,
    add_StreamAnalyzerServiceServicer_to_server,
)


@dataclass
class FrameAnalyzerResult:
    is_keyframe: bool = False
    timestamp: Timestamp = field(default_factory=Timestamp)
    thumbnail_data: bytes = b""
    labels: list[str] = field(
        default_factory=list
    )  # ここに設定されたラベルでAI Studio画面上でフィルタリングが可能
    data: dict = field(default_factory=dict)  # 任意のJSONシリアライズ可能なデータ
    score: float = 0.0  # ここに設定されたスコアでAI Studio画面上でフィルタリングが可能


def create_event_response(result: FrameAnalyzerResult) -> StreamAnalyzeResponse:
    return StreamAnalyzeResponse(
        record_event=StreamAnalyzeResponse.RecordEvent(
            timestamp=result.timestamp,
            type="detect.keyframe",
            event_index=str(result.timestamp.ToMilliseconds()),
            labels=result.labels,
            score=result.score,
            data=ParseDict(
                result.data,
                Struct(),
            ),
            geometry_config_ids=[1, 2, 3],
            picture=EventPicture(content_type="image/jpeg", data=result.thumbnail_data),
        )
    )


def create_object_response(result: FrameAnalyzerResult) -> StreamAnalyzeResponse:
    return StreamAnalyzeResponse(
        record_object=StreamAnalyzeResponse.RecordObject(
            start_timestamp=result.timestamp,
            end_timestamp=result.timestamp,
            type="detect.keyframe",
            object_index=str(result.timestamp.ToMilliseconds()),
            labels=result.labels,
            score=result.score,
            data=ParseDict(
                result.data,
                Struct(),
            ),
            geometry_config_ids=[1, 2, 3],
            picture=[
                ObjectPicture(
                    label=result.labels[0] if result.labels else "keyframe",
                    content_type="image/jpeg",
                    data=result.thumbnail_data,
                )
            ],
        )
    )


def create_metrics_response(result: FrameAnalyzerResult) -> StreamAnalyzeResponse:
    return StreamAnalyzeResponse(
        record_metrics=StreamAnalyzeResponse.RecordMetrics(
            timestamp=result.timestamp,
            units=["5minutes", "hourly", "daily"],
            metrics={
                "person": 1 if result.is_keyframe else 0,
                "random": random.random(),
            },
            daily_boundary_timezone="Asia/Tokyo",
        )
    )


def create_device_status_response(result: FrameAnalyzerResult) -> StreamAnalyzeResponse:
    return StreamAnalyzeResponse(
        record_device_status=StreamAnalyzeResponse.RecordDeviceStatus(
            timestamp=result.timestamp,
            device_status=[
                DeviceStatus(
                    label="keyframe",
                    status="detected",
                    score=result.score,
                    geometry_config_ids=[1, 2, 3],
                )
            ],
        )
    )


def create_update_context_response(context: dict) -> StreamAnalyzeResponse:
    return StreamAnalyzeResponse(
        update_context=StreamAnalyzeResponse.UpdateContext(
            context=ParseDict(
                context,
                Struct(),
            ),
        )
    )


class DummyObjectDetector:
    """ダミーの物体検出器"""

    def __init__(self, model_path: str) -> None:
        # NOTE: ここではコンストラクタでモデルをロードする想定でダミーを実装します。
        logging.info("Loaded dummy object detector model from %s", model_path)
        # self.model = load_model(model_path)

    def detect(self, frame: av.video.frame.VideoFrame) -> list[dict[str, object]]:
        # NOTE: ここでは常に同じ物体を検出するダミーを実装します。
        # input_img = frame.to_ndarray(format="bgr24")  # OpenCV形式の画像に変換
        # または
        # input_img = frame.to_image()  # PIL形式の画像に変換
        # detections = self.model.predict(input_img)
        detections = [
            {
                "label": "person",
                "score": 0.9,
                "top_x": 50,
                "top_y": 100,
                "bottom_x": 150,
                "bottom_y": 300,
            },
            {
                "label": "car",
                "score": 0.8,
                "top_x": 300,
                "top_y": 200,
                "bottom_x": 500,
                "bottom_y": 400,
            },
        ]
        return detections


class FrameAnalyzer:
    """アプリケーション固有の動画フレームの解析ロジックを実装します。

    このサンプルでは、動画のキーフレームを100回おきに検出し、ダミー推論およびサムネイル画像を生成します。
    """

    def __init__(
        self,
        device_id: str | bytes | None = None,
        device_context: str | bytes | None = None,
        parameters: str | bytes | None = None,
        frame_width: int | None = None,
        frame_height: int | None = None,
        fps: int | None = None,
    ):
        # NOTE: デバイスID、デバイスコンテキスト、パラメータなどを用いて解析処理を変更できます
        logging.info(
            "FrameAnalyzer got device_id=%s, device_context=%s, parameters=%s, frame_width=%s, frame_height=%s, fps=%s",
            device_id,
            device_context,
            parameters,
            frame_width,
            frame_height,
            fps,
        )
        self.key_frame_count = 0
        self.object_detector = DummyObjectDetector("dummy_model_path")
        # NOTE: デバイスコンテキストは通知間隔などカメラごとに内部で管理する情報を保持する想定です
        # デバイスコンテキストをcreate_update_context_responseで出力することで、セッションが切り替わっても情報を引き継げます
        self.is_updated_context = False
        self.device_context = {}
        if device_context is not None:
            try:
                self.device_context = json.loads(device_context)
            except json.JSONDecodeError:
                logging.warning("Failed to parse device_context as JSON.")
        self._user_conf: dict = {}  # ユーザー設定
        self._dev_conf: dict = {}  # デベロッパー設定
        if parameters is not None:
            try:
                params = json.loads(parameters)
                self._user_conf = params.get("user_config", {})
                self._dev_conf = params.get("developer_config", {})
                # NOTE: ジオメトリを使う場合
                # self._geometries: list[dict] = self._user_conf.get("geometries", [])
            except json.JSONDecodeError:
                logging.warning("Failed to parse parameters as JSON.")

    def analyze_frame(self, frame: av.video.frame.VideoFrame) -> FrameAnalyzerResult | None:
        if frame.pict_type == 0:  # Invalid Picture Type
            logging.debug("skip invalid frame")
            return None

        # NOTE: このサンプルではキーフレームを100回に1回解析する例ですが、デバイスコンテキストの情報を用いて処理間隔を制御することも可能です。
        self.key_frame_count += 1
        if self.key_frame_count % 100 != 0:
            return None
        # 推論を実行
        detections = self.object_detector.detect(frame)
        # NOTE: ここで検出結果をもとに解析ロジックを実装できます。user_configやdeveloper_configでラベルごとの閾値を調整することも可能です。

        # 結果を整形して返却
        # NOTE: dataフィールドには任意のJSONシリアライズ可能なデータを設定できます
        # ラベルのリストを設定することで画面上でのフィルタリングが可能になります
        # スコアを設定することで画面上でのフィルタリングが可能になります
        data = {"detections": detections}
        labels: list[str] = list({d["label"] for d in detections})
        score: float = max(d["score"] for d in detections) if detections else 0.0
        thumbnail_data = FrameAnalyzer.create_thumbnail(frame)
        ts = FrameAnalyzer.extract_timestamp(frame)
        # デバイスコンテキストを更新
        self.device_context["last_updated_at"] = ts.ToDatetime(tzinfo=datetime.timezone.utc).isoformat()
        self.is_updated_context = True
        return FrameAnalyzerResult(
            is_keyframe=frame.key_frame,
            timestamp=ts,
            thumbnail_data=thumbnail_data,
            data=data,
            labels=labels,
            score=score,
        )

    @staticmethod
    def create_thumbnail(frame: av.video.frame.VideoFrame) -> bytes:
        thumb_width = frame.width
        thumb_height = frame.height

        if frame.width > 640 or frame.height > 640:
            thumb_width = 640
            thumb_height = 640
            if frame.width > frame.height:
                thumb_height = frame.height * thumb_width // frame.width
            else:
                thumb_width = frame.width * thumb_height // frame.height

        logging.debug(
            "Creating thumbnail: original=%dx%d, thumbnail=%dx%d",
            frame.width,
            frame.height,
            thumb_width,
            thumb_height,
        )

        thumb_image: Image.Image = frame.to_image(width=thumb_width, height=thumb_height)
        thumb_bytes = io.BytesIO()
        thumb_image.save(thumb_bytes, format="JPEG")
        thumb_bytes.seek(0)

        return thumb_bytes.getvalue()

    @staticmethod
    def extract_timestamp(frame: av.video.frame.VideoFrame) -> Timestamp:
        ts = Timestamp()
        ts.FromDatetime(dt=datetime.datetime.fromtimestamp(frame.pts / 90000, tz=datetime.timezone.utc))
        return ts


class VideoDecoder:
    """H.264 動画ストリームのデコーダー"""

    def __init__(self):
        # H.264デコーダーを初期化
        self.vcodec = av.CodecContext.create("h264", "r")

    def decode_frame(self, request: StreamAnalyzeRequest) -> av.video.frame.VideoFrame | None:
        # 動画パケット以外は処理をスキップ
        if not VideoDecoder.is_video_packet(request):
            return None

        # 動画フレームのデコード
        p = av.Packet(request.media_frame.data)
        p.pts = request.media_frame.pts
        p.dts = request.media_frame.dts

        try:
            for frame in self.vcodec.decode(p):
                if frame is not None:
                    pts_datetime = datetime.datetime.fromtimestamp(request.media_frame.pts / 90000)
                    logging.debug(
                        "pts=%s, now=%s, frame width=%s height=%s format=%s pict_type=%s",
                        pts_datetime,
                        datetime.datetime.now(),
                        frame.width,
                        frame.height,
                        frame.format,
                        frame.pict_type,
                    )
                    return frame  # パケットには単一のフレームが含まれる
        except av.error.InvalidDataError:
            logging.warning("Invalid video packet data, skipping packet")
            return None

        return None

    @staticmethod
    def is_video_packet(request: StreamAnalyzeRequest) -> bool:
        return (request.media_frame.type & 0x01) == 0


class _StreamAnalyzer(StreamAnalyzerServiceServicer):
    """動画ストリーム解析を行います。"""

    def AnalyzeStream(
        self, request_iterator: Iterator[StreamAnalyzeRequest], context: grpc.ServicerContext
    ) -> Iterator[StreamAnalyzeResponse]:
        logging.info("accept %s", context.peer())

        metadata = dict(context.invocation_metadata())

        request_id = metadata.get("request_id")
        device_id = metadata.get("device_id")
        parameters = metadata.get("parameter")  # ユーザーおよびディベロッパーが指定したパラメータ情報を取得
        device_context = metadata.get("context")  # デバイスコンテキスト情報を取得
        video_width = metadata.get("stream.video_width")
        video_height = metadata.get("stream.video_height")
        fps = metadata.get("stream.video_frame_rate")
        assert isinstance(video_width, str) and isinstance(video_height, str) and isinstance(fps, str)
        video_width = int(video_width)
        video_height = int(video_height)
        fps = int(fps)
        # 動画フレーム解析用のオブジェクトを初期化。
        frame_analyzer = FrameAnalyzer(device_id, device_context, parameters, video_width, video_height, fps)

        logging.info("Start AnalyzeStream request_id=%s", request_id)

        context.send_initial_metadata((("analyzer_version", "v0.1.0"),))

        decoder = VideoDecoder()
        try:
            for r in request_iterator:
                frame = decoder.decode_frame(r)
                if frame is None:
                    continue

                # 動画フレームを解析
                result = frame_analyzer.analyze_frame(frame)

                if result:
                    # 解析結果をイベント・オブジェクト・メトリクス・デバイスステータスとして出力できます。
                    # いずれのデータ形式も毎フレーム出力することは想定されておらず、
                    # イベントは人間からみて十分に重要な事象のみ、メトリクスは1分おき程度の間隔で報告してください。
                    yield create_event_response(result)
                    yield create_object_response(result)
                    yield create_metrics_response(result)
                    yield create_device_status_response(result)
                if frame_analyzer.is_updated_context:
                    # NOTE: デバイスコンテキストを出力することで、セッションが切り替わっても情報を引き継げます
                    yield create_update_context_response(frame_analyzer.device_context)
                    frame_analyzer.is_updated_context = False

        except Exception:
            logging.exception("abort")
            raise
        else:
            logging.info("close")


@click.command()
@click.option("--address", default="[::]:50051")
def serve(address):
    """Safie AIソリューションプラットフォームのストリームAnalyzerのサンプル実装を起動します。"""

    # NOTE: 一つのAnalyzerインスタンスで同時に処理可能なデバイス数を指定してください
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(10))

    add_StreamAnalyzerServiceServicer_to_server(_StreamAnalyzer(), server)
    server.add_insecure_port(address)
    server.start()
    logging.info("server listening at %s", address)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
