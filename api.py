# 03/25書き換えテスト
from fastapi import FastAPI, Request
import requests
from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import pandas as pd
import os

app = FastAPI()

ACCESS_KEY = os.environ["APPSHEET_KEY"]
APP_ID = os.environ["APPSHEET_APP_ID"]
TABLE_NAME = "test"

# -----------------------------
# 距離（簡易）
# -----------------------------
def dist(a, b):
    return int(((a[0]-b[0])**2 + (a[1]-b[1])**2) * 100000)

# -----------------------------
# 最適化
# -----------------------------
def optimize(df):
    if len(df) == 0:
        return []

    coords = list(zip(df["lat"], df["lng"]))
    n = len(coords)

    manager = pywrapcp.RoutingIndexManager(n, 1, 0)
    routing = pywrapcp.RoutingModel(manager)

    def cb(i, j):
        return dist(
            coords[manager.IndexToNode(i)],
            coords[manager.IndexToNode(j)]
        )

    t = routing.RegisterTransitCallback(cb)
    routing.SetArcCostEvaluatorOfAllVehicles(t)

    routing.AddDimension(t, 0, 9999999, True, "D")
    dim = routing.GetDimensionOrDie("D")

    # ペア制約（受注ID）
    for oid in df["orderID"].unique():
        g = df[df["orderID"] == oid]

        if len(g) == 2:
            try:
                f = g[g["type"] == "from"].index[0]
                t_ = g[g["type"] == "to"].index[0]
            except:
                continue  # type不一致対策

            fi = manager.NodeToIndex(f)
            ti = manager.NodeToIndex(t_)

            routing.AddPickupAndDelivery(fi, ti)
            routing.solver().Add(routing.VehicleVar(fi) == routing.VehicleVar(ti))
            routing.solver().Add(dim.CumulVar(fi) <= dim.CumulVar(ti))

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    params.time_limit.seconds = 3

    sol = routing.SolveWithParameters(params)

    if sol is None:
        return list(range(n))  # fallback

    idx = routing.Start(0)
    order = []

    while not routing.IsEnd(idx):
        order.append(manager.IndexToNode(idx))
        idx = sol.Value(routing.NextVar(idx))

    return order

# -----------------------------
# AppSheet書き込み
# -----------------------------
def write_appsheet(rows):
    url = f"https://api.appsheet.com/api/v2/apps/{APP_ID}/tables/{TABLE_NAME}/Action"

    body = {
        "Action": "Edit",
        "Properties": {"Locale": "ja-JP"},
        "Rows": rows
    }

    headers = {
        "ApplicationAccessKey": ACCESS_KEY
    }

    r = requests.post(url, json=body, headers=headers)
    print("AppSheet response:", r.text)

# -----------------------------
# API
# -----------------------------
@app.post("/optimize")
async def run(req: Request):
    try:
        data = await req.json()
        print("受信:", data)

        df = pd.DataFrame(data.get("locations", []))

        if df.empty:
            return {"ok": False, "msg": "no data"}

        # 型修正（ここ重要）
        df["lat"] = df["lat"].astype(float)
        df["lng"] = df["lng"].astype(float)

        order = optimize(df)

        rows = []
        for i, idx in enumerate(order, 1):
            r = df.iloc[idx]
            rows.append({
                "RowID": r["rowId"],
                "順番": i
            })

        write_appsheet(rows)

        return {"ok": True, "count": len(rows)}

    except Exception as e:
        print("エラー:", str(e))
        return {"ok": False, "error": str(e)}