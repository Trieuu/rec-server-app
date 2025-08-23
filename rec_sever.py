import os
from pathlib import Path
from typing import Any, Dict, List, Union

from flask import Flask, jsonify, request, abort
from flask_cors import CORS

import recommendation as rec  # must be in same dir

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Load data.json (same folder) unless overridden by env var
THIS_DIR = Path(__file__).resolve().parent
DATA_PATH = Path(os.environ.get("REC_DATA_PATH", THIS_DIR / "data.json"))
rec.load_data(DATA_PATH)

def _get_results(user_id: Union[int, str]) -> List[Dict[str, Any]]:
    try:
        uid = int(user_id) 
        r_history, r_cross, r_occ, r_best = rec.rec_user_converter(uid)
        results = [r_history, r_cross, r_occ, r_best]
        print(f"[Recommendation] user_id={user_id} -> {[results[0].get('product_id',''), results[1].get('product_id',''), results[2].get('product_id',''), results[3].get('product_id','')]}") 
        print(results)
    except Exception as e:
        abort(400, description=f"Error generating recommendations: {e}")
    return [r_history, r_cross, r_occ, r_best]

@app.get("/healthz")
def health():
    return jsonify({"status": "ok"}), 200

@app.get("/api/v1/recommend/<user_id>")
def recommend_path(user_id: str):
    return jsonify(_get_results(user_id)), 200

@app.get("/api/v1/recommend")
def recommend_query():
    uid = request.args.get("user_id")
    if not uid:
        abort(400, description="Missing user_id")
    return jsonify(_get_results(uid)), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
