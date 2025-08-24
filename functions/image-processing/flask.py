from flask import Flask, request, jsonify
from flask_cors import CORS
import cv2
import numpy as np
from ultralytics import YOLO
import supervision as sv
import base64


class Config:
    YOLO_MODEL_PATH = "./model.pt"
    PORT = 5000
    CONFIDENCE = 0.5
    CORS_ALLOWED_ORIGINS = "*"


app = Flask(__name__)
app.config.from_object(Config)
CORS(app, resources={r"/*": {"origins": app.config["CORS_ALLOWED_ORIGINS"]}})

try:
    model = YOLO(app.config["YOLO_MODEL_PATH"])
    class_dict = model.names
except Exception as e:
    raise Exception(f"Model load failed: {str(e)}")


def generate_thumbnail_flask(image, max_width=200, max_height=200):
    h, w = image.shape[:2]
    scale = min(max_width / w, max_height / h)
    new_w, new_h = int(w * scale), int(h * scale)
    thumbnail = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_AREA)
    return thumbnail


def image_to_base64(image):
    ret, img_encoded = cv2.imencode(".jpg", image, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
    if not ret:
        raise Exception("Image encode failed")
    return base64.b64encode(img_encoded).decode("utf-8")


@app.route("/detect-bird", methods=["POST"])
def detect_bird():
    try:
        if "image" not in request.files:
            return jsonify({"code": 400, "msg": "No image file", "data": {}}), 400

        image_file = request.files["image"]
        img_bytes = image_file.read()
        img = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
        if img is None:
            return jsonify({"code": 400, "msg": "Image decode failed", "data": {}}), 400

        h, w = img.shape[:2]
        thickness = sv.calculate_optimal_line_thickness(resolution_wh=(w, h))
        text_scale = sv.calculate_optimal_text_scale(resolution_wh=(w, h))
        color_palette = sv.ColorPalette.from_matplotlib('magma', 10)
        box_annotator = sv.BoxAnnotator(thickness=thickness, color=color_palette)
        label_annotator = sv.LabelAnnotator(color=color_palette, text_scale=text_scale, text_thickness=thickness,
                                            text_position=sv.Position.TOP_LEFT)

        result = model(img)[0]
        detections = sv.Detections.from_ultralytics(result)
        detections = detections[(detections.confidence > app.config["CONFIDENCE"])]

        if detections.class_id is not None:
            labels = [f"{class_dict[cls_id]} {conf * 100:.2f}%" for cls_id, conf in
                      zip(detections.class_id, detections.confidence)]
            box_annotator.annotate(img, detections=detections)
            label_annotator.annotate(img, detections=detections, labels=labels)

        thumbnail = generate_thumbnail_flask(img)
        annotated_img_base64 = image_to_base64(img)
        thumbnail_img_base64 = image_to_base64(thumbnail)

        tag_count = {}
        if detections.class_id is not None:
            for cls_id in detections.class_id:
                bird_species = class_dict[cls_id]
                tag_count[bird_species] = tag_count.get(bird_species, 0) + 1

        return jsonify({
            "code": 200,
            "msg": "Detect success",
            "data": {
                "tags": tag_count,
                "annotated_image": annotated_img_base64,
                "thumbnail_image": thumbnail_img_base64
            }
        }), 200
    except Exception as e:
        return jsonify({"code": 500, "msg": f"Detect failed: {str(e)}", "data": {}}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=app.config["PORT"], debug=False)