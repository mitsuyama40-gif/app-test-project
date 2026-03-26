from fastapi import FastAPI, Request
import requests
from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import pandas as pd
import os
import math

app = FastAPI()

ACCESS_KEY = os.environ["APPSHEET_KEY"]
APP_ID = os.environ["APPSHEET_APP_ID"]

TABLE_NAME = "test"

# -----------------------------
# 距離（ハーバーサイン）
# -----------------------------
def haversine(a, b):
    R = 6371000
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return int(2 * R * math.asin(math.sqrt(h)))

# -----------------------------
# 距離行列（爆速）
# -----------------------------
def build_matrix(coords):
    n = len(coords)
    matrix = [[0]*n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i != j:
                matrix[i][j] = haversine(coords[i], coords[j])

    return matrix

# -----------------------------
# 最適化
# -----------------------------
def optimize(df):

    # ★ここ重要（変換のみ・APIなし）
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")

    # 欠損チェック
    if df["lat"].isna().any() or df["lng"].isna().any():
        raise Exception("緯度経度が空の行があります")

    coords = list(zip(df["lat"], df["lng"]))
    n = len(coords)

    manager = pywrapcp.RoutingIndexManager(n, 1, 0)
    routing = pywrapcp.RoutingModel(manager)

    matrix = build_matrix(coords)

    def cb(i, j):
        return matrix[manager.IndexToNode(i)][manager.IndexToNode(j)]

    t = routing.RegisterTransitCallback(cb)
    routing.SetArcCostEvaluatorOfAllVehicles(t)

    routing.AddDimension(t, 0, 9999999, True, "D")
    dim = routing.GetDimensionOrDie("D")

    # ペア制約
    for oid in df["orderID"].unique():
        g = df[df["orderID"] == oid]

        if len(g) == 2:
            f = g[g["type"] == "from"].index[0]
            t_ = g[g["type"] == "to"].index[0]

            fi = manager.NodeToIndex(f)
            ti = manager.NodeToIndex(t_)

            routing.AddPickupAndDelivery(fi, ti)
            routing.solver().Add(routing.VehicleVar(fi) == routing.VehicleVar(ti))
            routing.solver().Add(dim.CumulVar(fi) <= dim.CumulVar(ti))

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    params.time_limit.seconds = 1

    sol = routing.SolveWithParameters(params)

    if sol is None:
        return list(range(n))

    idx = routing.Start(0)
    order = []

    while not routing.IsEnd(idx):
        order.append(manager.IndexToNode(idx))
        idx = sol.Value(routing.NextVar(idx))

    return order

# -----------------------------
# 書き込み
# -----------------------------
def write_appsheet(rows):
    url = f"https://api.appsheet.com/api/v2/apps/{APP_ID}/tables/{TABLE_NAME}/Action"
    body = {"Action": "Edit", "Rows": rows}
    headers = {"ApplicationAccessKey": ACCESS_KEY}
    requests.post(url, json=body, headers=headers)

# -----------------------------
# API
# -----------------------------
@app.post("/optimize")
async def run(req: Request):

    data = await req.json()
    df = pd.DataFrame(data["locations"])

    order = optimize(df)

    rows = []
    for i, idx in enumerate(order, 1):
        r = df.iloc[idx]
        rows.append({
            "ID": r["rowId"],
            "順番": i
        })

    write_appsheet(rows)

    return {"ok": True}