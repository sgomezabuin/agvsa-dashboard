"""
app.py — Servidor Flask para el dashboard AGVSA.
Se despliega en Azure App Service.

Variables de entorno requeridas en Azure:
  COLPPY_API_USUARIO      (ej: ColppyAPI)
  COLPPY_API_PASSWORD_MD5 (hash MD5 del password de API)
  COLPPY_USUARIO          (ej: sgomezabuin@gmail.com)
  COLPPY_PASSWORD         (password de Colppy)
  COLPPY_ID_EMPRESA       (ej: 21006)

En local usa config.json como fallback.
"""

import hashlib, json, os, re, sys
import requests
from datetime import datetime, date, timedelta
from calendar import monthrange
from flask import Flask, jsonify, request, send_file, abort, session, redirect, url_for
from functools import wraps

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "agvsa-secret-2026")

# ─── Login ────────────────────────────────────────────────────────────────────
DASHBOARD_USER = os.environ.get("DASH_USER", "agvsa")
DASHBOARD_PASS = os.environ.get("DASH_PASS", "dashboard2026")

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

@app.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    if request.method == "POST":
        if (request.form.get("usuario") == DASHBOARD_USER and
                request.form.get("password") == DASHBOARD_PASS):
            session["logged_in"] = True
            nombre = (request.form.get("nombre") or "").strip() or "Usuario"
            session["nombre"] = nombre
            return redirect("/")
        error = "Usuario o contraseña incorrectos"
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AGVSA · Acceso</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background: #f0f4f8; display: flex; align-items: center;
        justify-content: center; min-height: 100vh; }}
.card {{ background: white; border-radius: 12px; padding: 40px;
         box-shadow: 0 4px 24px rgba(0,0,0,0.10); width: 100%; max-width: 360px; }}
h1 {{ font-size: 20px; color: #1e3a5f; margin-bottom: 6px; }}
p  {{ font-size: 13px; color: #64748b; margin-bottom: 28px; }}
label {{ font-size: 12px; font-weight: 600; color: #475569; display: block; margin-bottom: 5px; }}
input {{ width: 100%; padding: 10px 12px; border: 1px solid #e2e8f0;
         border-radius: 7px; font-size: 14px; margin-bottom: 16px; outline: none; }}
input:focus {{ border-color: #2563eb; }}
button {{ width: 100%; padding: 11px; background: #1e3a5f; color: white;
           border: none; border-radius: 7px; font-size: 14px;
           font-weight: 600; cursor: pointer; }}
button:hover {{ background: #2563eb; }}
.error {{ color: #dc2626; font-size: 12px; margin-bottom: 14px; }}
</style>
</head>
<body>
<div class="card">
  <h1>AGVSA · Dashboard</h1>
  <p>Ingresá tus credenciales para continuar</p>
  {'<div class="error">⚠ ' + error + '</div>' if error else ''}
  <form method="POST">
    <label>Tu nombre</label>
    <input type="text" name="nombre" placeholder="ej: Sebastian" autofocus />
    <label>Usuario</label>
    <input type="text" name="usuario" />
    <label>Contraseña</label>
    <input type="password" name="password" />
    <button type="submit">Ingresar</button>
  </form>
</div>
</body>
</html>"""

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

# ─── Rutas de datos ───────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG    = os.path.join(BASE_DIR, "config.json")
# En Azure App Service /home es persistente; en local usa el directorio del proyecto
DATA_PATH = os.environ.get("DATA_PATH",
            "/home/data.json" if os.path.exists("/home") and os.environ.get("WEBSITE_SITE_NAME")
            else os.path.join(BASE_DIR, "data.json"))
API_URL   = "https://login.colppy.com/lib/frontera2/service.php"


# ─── Helpers ──────────────────────────────────────────────────────────────────
def md5(t):
    return hashlib.md5(t.encode()).hexdigest()

def parsear_mes(s):
    for fmt in ("%Y-%m", "%m/%Y", "%m-%Y"):
        try:
            return datetime.strptime(s, fmt).replace(day=1)
        except:
            pass
    raise ValueError(f"Formato de mes inválido: '{s}'. Usá YYYY-MM")

def rango(mes):
    ultimo = monthrange(mes.year, mes.month)[1]
    return mes.strftime("%Y-%m-%d"), f"{mes.year}-{mes.month:02d}-{ultimo:02d}"


# ─── Config ───────────────────────────────────────────────────────────────────
def cargar_config():
    """Carga config desde variables de entorno; fallback a config.json."""
    env = {
        "colppy": {
            "api_usuario":      os.environ.get("COLPPY_API_USUARIO"),
            "api_password_md5": os.environ.get("COLPPY_API_PASSWORD_MD5"),
            "usuario":          os.environ.get("COLPPY_USUARIO"),
            "password":         os.environ.get("COLPPY_PASSWORD"),
            "id_empresa":       os.environ.get("COLPPY_ID_EMPRESA"),
        }
    }
    # Si todas las vars están en el entorno, usarlas
    if all(env["colppy"].values()):
        cfg = env
    else:
        # Fallback a config.json (local)
        if not os.path.exists(CONFIG):
            raise RuntimeError("No se encontró config.json ni variables de entorno de Colppy")
        cfg = json.load(open(CONFIG, encoding="utf-8"))

    # Cargar mapeos desde config.json si existe (en Azure también se sube)
    if os.path.exists(CONFIG):
        full = json.load(open(CONFIG, encoding="utf-8"))
        cfg.setdefault("mapeo_ingresos", full.get("mapeo_ingresos", {}))
        cfg.setdefault("mapeo_egresos",  full.get("mapeo_egresos",  {}))
    return cfg


# ─── Data.json ────────────────────────────────────────────────────────────────
def cargar_datos():
    if not os.path.exists(DATA_PATH):
        return {"generado": None, "meses": []}
    try:
        return json.load(open(DATA_PATH, encoding="utf-8"))
    except:
        return {"generado": None, "meses": []}

def guardar_datos(data, usuario=None):
    data["generado"]    = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    data["generado_por"] = usuario or session.get("nombre", "Sistema")
    os.makedirs(os.path.dirname(DATA_PATH) or ".", exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Colppy API ───────────────────────────────────────────────────────────────
class ColppyAPI:
    def __init__(self, cfg):
        self.cfg    = cfg
        self.sesion = None
        api_pwd = cfg.get("api_password_md5") or md5(cfg.get("api_password", ""))
        self.auth = {"usuario": cfg["api_usuario"], "password": api_pwd}

    def _post(self, provision, operacion, params, full_resp=False):
        body = {
            "auth":       self.auth,
            "service":    {"provision": provision, "operacion": operacion},
            "parameters": params
        }
        try:
            r = requests.post(API_URL, json=body, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Error de red: {e}")
        d    = r.json()
        resp = d.get("response", {})
        if not resp.get("success"):
            raise RuntimeError(f"Colppy: {resp.get('message', str(d))}")
        if full_resp:
            return resp
        return resp.get("data", [])

    def _sp(self):
        return {"sesion": self.sesion, "idEmpresa": self.cfg["id_empresa"]}

    def login(self):
        d = self._post("Usuario", "iniciar_sesion", {
            "usuario": self.cfg["usuario"], "password": md5(self.cfg["password"])
        })
        info = d[0] if isinstance(d, list) else d
        self.sesion = {
            "usuario":     self.cfg["usuario"],
            "userId":      info.get("userId") or info.get("idUsuario", ""),
            "claveSesion": info.get("claveSesion") or info.get("sessionId") or info.get("idSesion", "")
        }
        if not self.sesion["claveSesion"]:
            raise RuntimeError(f"No se obtuvo clave de sesión. Respuesta: {json.dumps(d)}")

    def logout(self):
        try:
            self._post("Usuario", "cerrar_sesion", self._sp())
        except:
            pass

    def _listar(self, provision, operacion, campo_fecha, desde, hasta):
        def _filtrar(docs):
            for doc in docs:
                fecha_doc = ""
                for cf in [campo_fecha, "fecha", "fechaEmision", "fechaContable"]:
                    v = doc.get(cf, "")
                    if v:
                        fecha_doc = str(v)[:10]
                        break
                if desde <= fecha_doc <= hasta:
                    yield doc

        PAGE = 1000   # registros por llamada
        MAX  = 20000  # tope de seguridad

        # 1) Obtener total real de registros
        resp1 = self._post(provision, operacion,
                           {**self._sp(), "start": 0, "limit": 1}, full_resp=True)
        total = int(resp1.get("total") or resp1.get("count") or resp1.get("totalCount") or 0)
        if total == 0:
            total = 5000

        total = min(total, MAX)

        # 2) Si cabe en una sola página, traerlo de una
        if total <= PAGE:
            d = self._post(provision, operacion,
                           {**self._sp(), "start": 0, "limit": PAGE})
            return list(_filtrar(d))

        # 3) Traer todas las páginas y filtrar en Python
        todos = []
        for start in range(0, total, PAGE):
            d = self._post(provision, operacion,
                           {**self._sp(), "start": start, "limit": PAGE})
            if not d:
                break
            todos.extend(d)

        return list(_filtrar(todos))

    def _listar_todo(self, provision, operacion):
        """Descarga TODOS los registros sin filtro de fecha (para procesar en Python)."""
        PAGE = 1000
        MAX  = 20000
        resp1 = self._post(provision, operacion,
                           {**self._sp(), "start": 0, "limit": 1}, full_resp=True)
        total = int(resp1.get("total") or resp1.get("count") or resp1.get("totalCount") or 0)
        if total == 0:
            total = 5000
        total = min(total, MAX)
        todos = []
        for start in range(0, total, PAGE):
            d = self._post(provision, operacion,
                           {**self._sp(), "start": start, "limit": PAGE})
            if not d:
                break
            todos.extend(d)
        return todos

    def facturas_venta(self, desde, hasta):
        return self._listar("FacturaVenta",  "listar_facturasventa",  "fechaFactura", desde, hasta)

    def facturas_compra(self, desde, hasta):
        return self._listar("FacturaCompra", "listar_facturascompra", "fechaFactura", desde, hasta)

    def movimientos_caja(self, desde, hasta):
        """Trae movimientos de caja/banco del período."""
        PAGE = 1000
        MAX  = 20000

        resp1 = self._post("MovimientoCaja", "listar_movimientoscaja",
                           {**self._sp(), "start": 0, "limit": 1}, full_resp=True)
        total = int(resp1.get("total") or 0)
        if total == 0:
            total = 5000
        total = min(total, MAX)

        def _filtrar(docs):
            for doc in docs:
                fecha_doc = str(doc.get("fecha", doc.get("fechaMovimiento", "")))[:10]
                if desde <= fecha_doc <= hasta:
                    yield doc

        todos = []
        for start in range(0, total, PAGE):
            d = self._post("MovimientoCaja", "listar_movimientoscaja",
                           {**self._sp(), "start": start, "limit": PAGE})
            if not d:
                break
            todos.extend(d)

        return list(_filtrar(todos))

    def cuentas_cobrar(self):
        """Trae TODAS las facturas de venta impagas (idEstadoFactura != 5 = pagada)."""
        PAGE = 1000
        MAX  = 20000

        resp1 = self._post("FacturaVenta", "listar_facturasventa",
                           {**self._sp(), "start": 0, "limit": 1}, full_resp=True)
        total = int(resp1.get("total") or 5000)
        total = min(total, MAX)

        todos = []
        for start in range(0, total, PAGE):
            d = self._post("FacturaVenta", "listar_facturasventa",
                           {**self._sp(), "start": start, "limit": PAGE})
            if not d:
                break
            todos.extend(d)

        # Estado 5 = cobrada, otros = pendiente
        pendientes = [d for d in todos
                      if str(d.get("idEstadoFactura","")) != "5"
                      and float(d.get("totalFactura", 0) or 0) > 0]
        return pendientes


# ─── Mapeo ────────────────────────────────────────────────────────────────────
def nombre_doc(doc):
    for k in ("RazonSocial", "NombreFantasia", "razonSocialCliente",
              "razonSocial", "nombreProveedor", "razonSocialProveedor", "descripcion"):
        v = doc.get(k, "")
        if v:
            return str(v).strip()
    return "(sin nombre)"

def monto_doc(doc):
    for k in ("totalFactura", "total", "importe", "monto"):
        v = doc.get(k)
        if v is not None:
            monto = float(v)
            tipo  = str(doc.get("tipoFactura", doc.get("tipo", ""))).upper()
            return -abs(monto) if ("NC" in tipo or "NOTA DE CRED" in tipo) else monto
    return 0.0

def agrupar(docs, mapeo):
    """Agrupa documentos por categoría.
    Retorna: (totales, sin_mapear, ranking_lista, todos_lista)
    - totales:       {categoria: monto}
    - sin_mapear:    [{nombre, monto, tipo}]  — no encontraron categoría
    - ranking_lista: [{nombre, monto}]         — sólo los categorizados, por nombre
    - todos_lista:   [{nombre, monto, categoria}] — TODOS, con o sin categoría
    """
    totales    = {}
    sin_mapear = []
    ranking    = {}   # nombre → monto  (sólo categorizados)
    todos      = {}   # nombre → {monto, categoria}

    for doc in docs:
        nombre_u = nombre_doc(doc).upper()
        nombre_d = nombre_doc(doc)
        monto    = monto_doc(doc)
        cat      = None
        for k, palabras in mapeo.items():
            if k.startswith("_"):
                continue
            for p in (palabras if isinstance(palabras, list) else [palabras]):
                if p and not p.startswith("TODO") and p.upper() in nombre_u:
                    cat = k
                    break
            if cat:
                break

        if nombre_d not in todos:
            todos[nombre_d] = {"monto": 0.0, "categoria": cat or ""}
        todos[nombre_d]["monto"]    += monto
        todos[nombre_d]["categoria"] = todos[nombre_d]["categoria"] or cat or ""

        if cat:
            totales[cat]      = totales.get(cat, 0.0) + monto
            ranking[nombre_d] = ranking.get(nombre_d, 0.0) + monto
        else:
            sin_mapear.append({
                "nombre": nombre_d,
                "monto":  monto,
                "tipo":   doc.get("tipoFactura", doc.get("tipo", ""))
            })

    ranking_lista = sorted(
        [{"nombre": k, "monto": round(v, 2)} for k, v in ranking.items()],
        key=lambda x: -x["monto"]
    )
    todos_lista = sorted(
        [{"nombre": k, "monto": round(v["monto"], 2), "categoria": v["categoria"]}
         for k, v in todos.items()],
        key=lambda x: -x["monto"]
    )
    return totales, sin_mapear, ranking_lista, todos_lista


def procesar_mes(fv_todos, fc_todos, periodo, cfg):
    """Filtra las facturas ya descargadas para un mes y devuelve mes_data."""
    from calendar import monthrange as _mr
    mes_dt   = datetime.strptime(periodo, "%Y-%m")
    ultimo   = _mr(mes_dt.year, mes_dt.month)[1]
    desde    = f"{periodo}-01"
    hasta    = f"{periodo}-{ultimo:02d}"

    def _filtrar(docs, campo_fecha):
        for doc in docs:
            fecha_doc = ""
            for cf in [campo_fecha, "fecha", "fechaEmision", "fechaContable"]:
                v = doc.get(cf, "")
                if v:
                    fecha_doc = str(v)[:10]
                    break
            if desde <= fecha_doc <= hasta:
                yield doc

    fv = list(_filtrar(fv_todos, "fechaFactura"))
    fc = list(_filtrar(fc_todos, "fechaFactura"))

    ing, sin_ing, rank_ing, todos_ing = agrupar(fv, cfg.get("mapeo_ingresos", {}))
    egr, sin_egr, rank_egr, todos_egr = agrupar(fc, cfg.get("mapeo_egresos",  {}))

    retiro_de_colppy = egr.pop("retiro_socio", None)
    rank_egr = [r for r in rank_egr if buscar_categoria(r["nombre"], cfg.get("mapeo_egresos", {})) != "retiro_socio"]

    def _acum_sin(lista):
        acc = {}
        for x in lista:
            acc[x["nombre"]] = acc.get(x["nombre"], 0) + x["monto"]
        return [{"nombre": k, "monto": round(v, 2)} for k, v in sorted(acc.items(), key=lambda i: -i[1])]

    otros_ing = round(sum(x["monto"] for x in sin_ing), 2)
    otros_egr = round(sum(x["monto"] for x in sin_egr), 2)

    mes_data = {
        "periodo":    periodo,
        "ingresos":   ing,
        "egresos":    egr,
        "otros_ingresos": otros_ing,
        "otros_egresos":  otros_egr,
        "detalles":   {"consorcios": rank_ing, "proveedores": rank_egr},
        "sin_mapear": {"venta": _acum_sin(sin_ing), "compra": _acum_sin(sin_egr)},
        "todos":      {"venta": todos_ing, "compra": todos_egr},
        "fv_count":   len(fv),
        "fc_count":   len(fc),
        "sin_mapear_raw": {"venta": sin_ing, "compra": sin_egr},
    }
    if retiro_de_colppy is not None:
        mes_data["retiro_socio"] = round(retiro_de_colppy, 2)

    return mes_data


# ─── Rutas Flask ──────────────────────────────────────────────────────────────
@app.route("/")
@login_required
def index():
    return send_file(os.path.join(BASE_DIR, "dashboard.html"))

@app.route("/api/datos")
@login_required
def api_datos():
    return jsonify(cargar_datos())

@app.route("/api/actualizar_rango", methods=["POST"])
@login_required
def api_actualizar_rango():
    """Descarga TODAS las facturas una sola vez y procesa cada mes del rango en Python."""
    body   = request.get_json() or {}
    desde  = body.get("desde", "")   # YYYY-MM
    hasta  = body.get("hasta", "")   # YYYY-MM

    if not desde or not hasta:
        return jsonify({"ok": False, "error": "Faltan campos: desde, hasta"}), 400

    # Generar lista de meses del rango
    def rango_meses(d, h):
        meses = []
        y, m  = int(d[:4]), int(d[5:7])
        yf,mf = int(h[:4]), int(h[5:7])
        while (y, m) <= (yf, mf):
            meses.append(f"{y}-{m:02d}")
            m += 1
            if m > 12: m, y = 1, y + 1
        return meses

    periodos = rango_meses(desde, hasta)
    if not periodos:
        return jsonify({"ok": False, "error": "Rango vacío"}), 400

    try:
        cfg = cargar_config()
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    api = ColppyAPI(cfg["colppy"])
    try:
        api.login()
        # Una sola descarga de TODAS las facturas
        fv_todos = api._listar_todo("FacturaVenta",  "listar_facturasventa")
        fc_todos = api._listar_todo("FacturaCompra", "listar_facturascompra")
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    finally:
        api.logout()

    data  = cargar_datos()
    meses = data.get("meses", [])

    resultados = []
    for periodo in periodos:
        mes_data = procesar_mes(fv_todos, fc_todos, periodo, cfg)

        # TC automático
        tc = {}
        mes_dt = datetime.strptime(periodo, "%Y-%m")
        blue_v, ofic_v, _ = _fetch_tc_mes(mes_dt.year, mes_dt.month)
        if blue_v:
            tc["blue"]    = blue_v
            tc["oficial"] = ofic_v
        if tc:
            mes_data["tipo_cambio"] = tc

        # Preservar retiro_socio y TC manual si ya existían y no vinieron de Colppy
        idx = next((i for i, m in enumerate(meses) if m["periodo"] == periodo), None)
        if idx is not None:
            existente = meses[idx]
            for campo in ("tipo_cambio", "retiro_socio"):
                if campo not in mes_data and campo in existente:
                    mes_data[campo] = existente[campo]
            meses[idx] = mes_data
        else:
            meses.append(mes_data)

        sin_raw = mes_data.pop("sin_mapear_raw", {})
        resultados.append({
            "periodo":   periodo,
            "fv_count":  mes_data["fv_count"],
            "fc_count":  mes_data["fc_count"],
            "sin_venta": len(sin_raw.get("venta", [])),
            "sin_compra":len(sin_raw.get("compra", [])),
        })
        mes_data.pop("fv_count", None)
        mes_data.pop("fc_count", None)

    # Fecha del comprobante más reciente en Colppy
    ultima_fecha_colppy = None
    for doc in (fv_todos + fc_todos):
        for cf in ["fechaFactura", "fecha", "fechaEmision", "fechaContable"]:
            v = doc.get(cf)
            if v:
                f = str(v)[:10]
                if not ultima_fecha_colppy or f > ultima_fecha_colppy:
                    ultima_fecha_colppy = f
                break

    data["meses"] = meses
    data["ultima_sync_colppy"]    = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    data["ultima_sync_colppy_por"] = session.get("nombre", "Usuario")
    if ultima_fecha_colppy:
        data["ultima_fecha_colppy"] = ultima_fecha_colppy
    guardar_datos(data)

    return jsonify({"ok": True, "meses": len(periodos), "detalle": resultados, "data": data})


@app.route("/api/actualizar", methods=["POST"])
@login_required
def api_actualizar():
    body = request.get_json() or {}
    mes_str      = body.get("mes", "")
    dolar_blue   = body.get("dolar_blue")
    dolar_ofic   = body.get("dolar_oficial")
    retiro_socio = body.get("retiro_socio")

    if not mes_str:
        return jsonify({"ok": False, "error": "Falta el campo 'mes' (ej: 2026-03)"}), 400

    try:
        mes      = parsear_mes(mes_str)
        desde, hasta = rango(mes)
        periodo  = mes.strftime("%Y-%m")
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    try:
        cfg = cargar_config()
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    api = ColppyAPI(cfg["colppy"])
    try:
        api.login()
        fv = api.facturas_venta(desde, hasta)
        fc = api.facturas_compra(desde, hasta)
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    finally:
        api.logout()

    ing, sin_ing, rank_ing, todos_ing = agrupar(fv, cfg.get("mapeo_ingresos", {}))
    egr, sin_egr, rank_egr, todos_egr = agrupar(fc, cfg.get("mapeo_egresos",  {}))

    # "retiro_socio" es una categoría especial en egresos — sacarla del cuadro de egresos
    retiro_de_colppy = egr.pop("retiro_socio", None)
    rank_egr = [r for r in rank_egr if buscar_categoria(r["nombre"], cfg.get("mapeo_egresos", {})) != "retiro_socio"]

    # Acumular sin_mapear: agrupar por nombre y sumar montos
    def _acum_sin(lista):
        acc = {}
        for x in lista:
            n = x["nombre"]
            acc[n] = acc.get(n, 0) + x["monto"]
        return [{"nombre": k, "monto": round(v, 2)} for k, v in sorted(acc.items(), key=lambda i: -i[1])]

    mes_data = {
        "periodo":    periodo,
        "ingresos":   ing,
        "egresos":    egr,
        "detalles":   {"consorcios": rank_ing, "proveedores": rank_egr},
        "sin_mapear": {"venta": _acum_sin(sin_ing), "compra": _acum_sin(sin_egr)},
        "todos":      {"venta": todos_ing, "compra": todos_egr},
    }
    tc = {}
    if dolar_blue   is not None: tc["blue"]    = float(dolar_blue)
    if dolar_ofic   is not None: tc["oficial"] = float(dolar_ofic)
    # Si no se ingresó TC manual, buscarlo automáticamente
    if not tc:
        blue_v, ofic_v, _ = _fetch_tc_mes(mes.year, mes.month)
        if blue_v:
            tc["blue"]    = blue_v
            tc["oficial"] = ofic_v

    if tc:                       mes_data["tipo_cambio"]  = tc
    # Retiro socio: primero desde Colppy (SGA), luego manual si lo ingresaron
    if retiro_de_colppy is not None:
        mes_data["retiro_socio"] = round(retiro_de_colppy, 2)
    elif retiro_socio is not None:
        mes_data["retiro_socio"] = float(retiro_socio)

    # Guardar en data.json
    data  = cargar_datos()
    meses = data.get("meses", [])
    idx   = next((i for i, m in enumerate(meses) if m["periodo"] == periodo), None)
    if idx is not None:
        existente = meses[idx]
        for campo in ("tipo_cambio", "retiro_socio"):
            if campo not in mes_data and campo in existente:
                mes_data[campo] = existente[campo]
        meses[idx] = mes_data
    else:
        meses.append(mes_data)
    data["meses"] = meses
    guardar_datos(data)

    return jsonify({
        "ok":          True,
        "periodo":     periodo,
        "fv_count":    len(fv),
        "fc_count":    len(fc),
        "sin_mapear":  {"venta": sin_ing, "compra": sin_egr},
        "data":        data
    })

@app.route("/api/cuentas_cobrar")
@login_required
def api_cuentas_cobrar():
    try:
        cfg = cargar_config()
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    api = ColppyAPI(cfg["colppy"])
    try:
        api.login()
        pendientes = api.cuentas_cobrar()
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    finally:
        api.logout()

    # Enriquecer con nombre y monto
    resultado = []
    for doc in pendientes:
        resultado.append({
            "nombre":  nombre_doc(doc),
            "monto":   monto_doc(doc),
            "fecha":   doc.get("fechaFactura", doc.get("fecha", "")),
            "nro":     doc.get("nroFactura", doc.get("numero", "")),
            "estado":  doc.get("idEstadoFactura", ""),
        })
    # Ordenar por monto descendente
    resultado.sort(key=lambda x: -x["monto"])
    total = sum(r["monto"] for r in resultado)
    return jsonify({"ok": True, "total": round(total, 2), "facturas": resultado})


@app.route("/api/estado")
@login_required
def api_estado():
    data = cargar_datos()
    return jsonify({
        "ok":      True,
        "meses":   len(data.get("meses", [])),
        "generado": data.get("generado")
    })


def buscar_categoria(nombre, mapeo):
    """Busca la categoría de un nombre según el mapeo actual de config.json."""
    nombre_u = nombre.upper()
    for k, palabras in mapeo.items():
        if k.startswith("_"):
            continue
        for p in (palabras if isinstance(palabras, list) else [palabras]):
            if p and not p.startswith("TODO") and p.upper() in nombre_u:
                return k
    return ""


@app.route("/api/sin_mapear")
@login_required
def api_sin_mapear():
    """Devuelve todos los nombres acumulados, re-categorizados con config.json actual."""
    data = cargar_datos()
    try:
        cfg = cargar_config()
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    mapeo_ing = cfg.get("mapeo_ingresos", {})
    mapeo_egr = cfg.get("mapeo_egresos",  {})
    cats_ing  = [k for k in mapeo_ing if not k.startswith("_")]
    cats_egr  = [k for k in mapeo_egr if not k.startswith("_")]

    # Acumular montos por nombre de todos los meses
    acum_venta  = {}  # nombre → monto
    acum_compra = {}

    for mes in data.get("meses", []):
        todos = mes.get("todos", {})
        for x in todos.get("venta", []):
            n = x["nombre"]
            acum_venta[n] = acum_venta.get(n, 0.0) + x["monto"]
        for x in todos.get("compra", []):
            n = x["nombre"]
            acum_compra[n] = acum_compra.get(n, 0.0) + x["monto"]
        # Fallback: meses sin campo "todos" usan sin_mapear
        if not todos:
            sin = mes.get("sin_mapear", {})
            for x in sin.get("venta", []):
                acum_venta[x["nombre"]] = acum_venta.get(x["nombre"], 0.0) + x["monto"]
            for x in sin.get("compra", []):
                acum_compra[x["nombre"]] = acum_compra.get(x["nombre"], 0.0) + x["monto"]

    # Re-aplicar config.json actual para saber la categoría de cada nombre ahora mismo
    def fmt(acum, mapeo):
        result = []
        for nombre, monto in acum.items():
            result.append({
                "nombre":    nombre,
                "monto":     round(monto, 2),
                "categoria": buscar_categoria(nombre, mapeo),  # siempre fresco de config.json
            })
        return sorted(result, key=lambda x: -x["monto"])

    return jsonify({
        "ok":            True,
        "cats_ingresos": cats_ing,
        "cats_egresos":  cats_egr,
        "venta":         fmt(acum_venta,  mapeo_ing),
        "compra":        fmt(acum_compra, mapeo_egr),
    })


def reprocesar_data(data, cfg):
    """Re-agrega ingresos/egresos usando config.json actual.
    Si el mes tiene 'todos' (datos completos), reprocesa todo.
    Si solo tiene 'sin_mapear' (meses viejos), mueve los ahora-categorizados a ingresos/egresos.
    """
    mapeo_ing = cfg.get("mapeo_ingresos", {})
    mapeo_egr = cfg.get("mapeo_egresos",  {})

    for mes in data.get("meses", []):
        todos = mes.get("todos", {})

        if todos:
            # ── Mes nuevo: reprocesar todo desde 'todos' ──────────────────
            ing, egr   = {}, {}
            sin_ing, sin_egr = [], []
            rank_ing, rank_egr = {}, {}
            todos_ing, todos_egr = [], []

            for x in todos.get("venta", []):
                cat = buscar_categoria(x["nombre"], mapeo_ing)
                if cat:
                    ing[cat]              = ing.get(cat, 0.0) + x["monto"]
                    rank_ing[x["nombre"]] = rank_ing.get(x["nombre"], 0.0) + x["monto"]
                else:
                    sin_ing.append({"nombre": x["nombre"], "monto": x["monto"]})
                todos_ing.append({"nombre": x["nombre"], "monto": round(x["monto"], 2), "categoria": cat})

            for x in todos.get("compra", []):
                cat = buscar_categoria(x["nombre"], mapeo_egr)
                if cat:
                    egr[cat]              = egr.get(cat, 0.0) + x["monto"]
                    rank_egr[x["nombre"]] = rank_egr.get(x["nombre"], 0.0) + x["monto"]
                else:
                    sin_egr.append({"nombre": x["nombre"], "monto": x["monto"]})
                todos_egr.append({"nombre": x["nombre"], "monto": round(x["monto"], 2), "categoria": cat})

            # Retiro socio especial: sacarlo de egresos
            retiro_col = egr.pop("retiro_socio", None)
            if retiro_col is not None:
                mes["retiro_socio"] = round(retiro_col, 2)

            mes["ingresos"]   = {k: round(v, 2) for k, v in ing.items()}
            mes["egresos"]    = {k: round(v, 2) for k, v in egr.items()}
            mes["detalles"]   = {
                "consorcios":  sorted([{"nombre": k, "monto": round(v,2)} for k,v in rank_ing.items()], key=lambda x:-x["monto"]),
                "proveedores": sorted([{"nombre": k, "monto": round(v,2)} for k,v in rank_egr.items() if k != "retiro_socio"], key=lambda x:-x["monto"]),
            }
            mes["sin_mapear"] = {
                "venta":  sorted(sin_ing, key=lambda x: -x["monto"]),
                "compra": sorted(sin_egr, key=lambda x: -x["monto"]),
            }
            mes["todos"] = {"venta": todos_ing, "compra": todos_egr}

        else:
            # ── Mes viejo: solo mueve los ahora-categorizados fuera de sin_mapear ──
            sin = mes.get("sin_mapear", {})

            nuevos_sin_v = []
            for x in sin.get("venta", []):
                cat = buscar_categoria(x["nombre"], mapeo_ing)
                if cat:
                    mes["ingresos"][cat] = round(mes["ingresos"].get(cat, 0.0) + x["monto"], 2)
                else:
                    nuevos_sin_v.append(x)
            sin["venta"] = nuevos_sin_v

            nuevos_sin_c = []
            for x in sin.get("compra", []):
                cat = buscar_categoria(x["nombre"], mapeo_egr)
                if cat == "retiro_socio":
                    mes["retiro_socio"] = round(mes.get("retiro_socio", 0.0) + x["monto"], 2)
                elif cat:
                    mes["egresos"][cat] = round(mes["egresos"].get(cat, 0.0) + x["monto"], 2)
                else:
                    nuevos_sin_c.append(x)
            sin["compra"] = nuevos_sin_c

            mes["sin_mapear"] = sin

    return data


@app.route("/api/asignar_categoria", methods=["POST"])
@login_required
def api_asignar_categoria():
    """Agrega un nombre al mapeo de config.json y reprocesa data.json."""
    body   = request.get_json() or {}
    nombre = (body.get("nombre") or "").strip()
    tipo   = body.get("tipo", "")      # "venta" o "compra"
    cat    = (body.get("categoria") or "").strip()

    if not nombre or not tipo or not cat:
        return jsonify({"ok": False, "error": "Faltan campos: nombre, tipo, categoria"}), 400

    if not os.path.exists(CONFIG):
        return jsonify({"ok": False, "error": "No se encontró config.json"}), 500

    # 1) Actualizar config.json
    cfg       = json.load(open(CONFIG, encoding="utf-8"))
    mapeo_key = "mapeo_ingresos" if tipo == "venta" else "mapeo_egresos"
    mapeo     = cfg.setdefault(mapeo_key, {})

    if cat not in mapeo:
        mapeo[cat] = []
    entrada = mapeo[cat]
    if isinstance(entrada, str):
        entrada = [entrada]
    if nombre.upper() not in [p.upper() for p in entrada]:
        entrada.append(nombre)
    mapeo[cat] = entrada

    with open(CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    # 2) Reprocesar data.json con los nuevos patrones
    data = cargar_datos()
    data = reprocesar_data(data, cfg)
    guardar_datos(data)

    return jsonify({"ok": True, "categoria": cat, "patron": nombre, "tipo": tipo})


def _fetch_tc_mes(anio, mes):
    """Obtiene blue y oficial del último día hábil del mes.
    Fuentes en orden: argentinadatos.com (historial completo) → bluelytics evolution → bluelytics latest.
    """
    ultimo = monthrange(anio, mes)[1]
    d_ult  = date(anio, mes, ultimo)
    while d_ult.weekday() >= 5:
        d_ult -= timedelta(days=1)
    fecha_iso = d_ult.strftime("%Y-%m-%d")   # YYYY-MM-DD
    mes_prefix = f"{anio}-{mes:02d}"         # YYYY-MM para filtrar

    blue_val = oficial_val = None

    # ── Intento 1: argentinadatos.com (historial completo en YYYY-MM-DD) ─────
    try:
        for tipo, asign in [("blue", "blue"), ("oficial", "oficial")]:
            url = f"https://api.argentinadatos.com/v1/cotizaciones/dolares/{tipo}"
            r   = requests.get(url, timeout=10)
            if r.ok:
                hist = r.json()   # [{fecha: "YYYY-MM-DD", compra: X, venta: Y}, ...]
                # Filtrar filas del mes y quedarse con la más cercana al último día hábil
                del_mes = [h for h in hist if str(h.get("fecha","")).startswith(mes_prefix)]
                if del_mes:
                    candidata = max(
                        (h for h in del_mes if h.get("fecha","") <= fecha_iso),
                        key=lambda h: h["fecha"],
                        default=None
                    )
                    if candidata:
                        val = round((float(candidata["compra"]) + float(candidata["venta"])) / 2, 2)
                        if asign == "blue":    blue_val    = val
                        else:                  oficial_val = val
    except Exception:
        pass

    # ── Intento 2: bluelytics evolution  ─────────────────────────────────────
    # Estructura: [{date, source:"Blue"|"Oficial", value_buy, value_sell}, ...]
    if blue_val is None:
        try:
            r = requests.get("https://api.bluelytics.com.ar/v2/evolution.json", timeout=10)
            if r.ok:
                hist = r.json()
                for source, asign in [("Blue", "blue"), ("Oficial", "oficial")]:
                    filas = [h for h in hist
                             if h.get("source") == source and h.get("date","") <= fecha_iso]
                    if filas:
                        mejor = max(filas, key=lambda h: h["date"])
                        val   = round((mejor["value_buy"] + mejor["value_sell"]) / 2, 2)
                        if asign == "blue":    blue_val    = val
                        else:                  oficial_val = val
        except Exception:
            pass

    # ── Intento 3: bluelytics latest (solo sirve para el mes actual) ─────────
    if blue_val is None:
        try:
            r = requests.get("https://api.bluelytics.com.ar/v2/latest", timeout=8)
            if r.ok:
                d = r.json()
                blue_val    = round((d["blue"]["value_buy"]    + d["blue"]["value_sell"])    / 2, 2)
                oficial_val = round((d["oficial"]["value_buy"] + d["oficial"]["value_sell"]) / 2, 2)
        except Exception:
            pass

    return blue_val, oficial_val, fecha_iso


@app.route("/api/dolar/debug")
@login_required
def api_dolar_debug():
    """Muestra la respuesta cruda de cada API para diagnosticar."""
    mes_str = request.args.get("mes", "2025-03")
    mes_dt  = parsear_mes(mes_str)
    anio, mes = mes_dt.year, mes_dt.month
    resultado = {}

    # Ambito blue
    try:
        url = f"https://mercados.ambito.com//dolar/informal/historico-extended/{anio}/{mes:02d}"
        r   = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resultado["ambito_blue_status"] = r.status_code
        resultado["ambito_blue_sample"] = r.json()[:3] if r.ok else r.text[:200]
    except Exception as e:
        resultado["ambito_blue_error"] = str(e)

    # Ambito oficial
    try:
        url = f"https://mercados.ambito.com//dolar/oficial/historico-extended/{anio}/{mes:02d}"
        r   = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resultado["ambito_ofic_status"] = r.status_code
        resultado["ambito_ofic_sample"] = r.json()[:3] if r.ok else r.text[:200]
    except Exception as e:
        resultado["ambito_ofic_error"] = str(e)

    # Bluelytics latest
    try:
        r = requests.get("https://api.bluelytics.com.ar/v2/latest", timeout=8)
        resultado["bluelytics_latest_status"] = r.status_code
        resultado["bluelytics_latest"] = r.json() if r.ok else r.text[:200]
    except Exception as e:
        resultado["bluelytics_latest_error"] = str(e)

    # Bluelytics evolution
    try:
        r = requests.get("https://api.bluelytics.com.ar/v2/evolution.json", timeout=10)
        resultado["bluelytics_evol_status"] = r.status_code
        data = r.json() if r.ok else []
        resultado["bluelytics_evol_sample"] = data[:2] if isinstance(data, list) else data
    except Exception as e:
        resultado["bluelytics_evol_error"] = str(e)

    return jsonify(resultado)


@app.route("/api/actualizar_tc", methods=["POST"])
@login_required
def api_actualizar_tc():
    """Recorre todos los meses en data.json y carga el TC de cada uno sin tocar Colppy."""
    data  = cargar_datos()
    lista = data.get("meses", [])
    if not lista:
        return jsonify({"ok": False, "error": "No hay datos guardados. Actualizá primero desde Colppy."})

    actualizados, errores = [], []
    for mes_data in sorted(lista, key=lambda x: x.get("periodo", "")):
        periodo = mes_data.get("periodo", "")
        if not periodo:
            continue
        try:
            mes_dt = parsear_mes(periodo)
            blue, oficial, fecha = _fetch_tc_mes(mes_dt.year, mes_dt.month)
            if blue:
                tc = {"blue": blue}
                if oficial:
                    tc["oficial"] = oficial
                mes_data["tipo_cambio"] = tc
                actualizados.append({"mes": periodo, "fecha": fecha, "blue": blue, "oficial": oficial})
            else:
                errores.append({"mes": periodo, "error": "No se obtuvo TC"})
        except Exception as ex:
            errores.append({"mes": periodo, "error": str(ex)})

    guardar_datos(data)
    return jsonify({"ok": True, "actualizados": len(actualizados), "errores": len(errores),
                    "detalle": actualizados, "errores_detalle": errores})


@app.route("/api/dolar")
@login_required
def api_dolar():
    """Devuelve el dólar blue y oficial del último día hábil del mes solicitado."""
    mes_str = request.args.get("mes", "")
    if not mes_str:
        return jsonify({"ok": False, "error": "Falta parámetro mes (YYYY-MM)"}), 400
    try:
        mes_dt = parsear_mes(mes_str)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    blue, oficial, fecha = _fetch_tc_mes(mes_dt.year, mes_dt.month)
    if blue:
        return jsonify({"ok": True, "mes": mes_str, "fecha": fecha, "blue": blue, "oficial": oficial})
    else:
        return jsonify({"ok": False, "error": "No se pudo obtener el tipo de cambio"})


@app.route("/api/flujo_caja")
@login_required
def api_flujo_caja():
    mes_str = request.args.get("mes", "")
    if not mes_str:
        # Default: mes actual
        from datetime import date
        mes_str = date.today().strftime("%Y-%m")
    try:
        mes    = parsear_mes(mes_str)
        desde, hasta = rango(mes)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    try:
        cfg = cargar_config()
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    api = ColppyAPI(cfg["colppy"])
    try:
        api.login()
        movs = api.movimientos_caja(desde, hasta)
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    finally:
        api.logout()

    ingresos  = [m for m in movs if float(m.get("debe", 0) or 0) > 0]
    egresos   = [m for m in movs if float(m.get("haber", 0) or 0) > 0]
    total_ing = sum(float(m.get("debe",  0) or 0) for m in ingresos)
    total_egr = sum(float(m.get("haber", 0) or 0) for m in egresos)

    def fmt_mov(m):
        return {
            "fecha":       str(m.get("fecha", m.get("fechaMovimiento", "")))[:10],
            "descripcion": m.get("descripcion", m.get("concepto", m.get("detalle", ""))),
            "debe":        float(m.get("debe",  0) or 0),
            "haber":       float(m.get("haber", 0) or 0),
            "saldo":       float(m.get("saldo", 0) or 0),
            "cuenta":      m.get("nombreCuenta", m.get("cuenta", "")),
        }

    return jsonify({
        "ok":        True,
        "periodo":   mes.strftime("%Y-%m"),
        "total_ingresos": round(total_ing, 2),
        "total_egresos":  round(total_egr, 2),
        "saldo_neto":     round(total_ing - total_egr, 2),
        "movimientos":    [fmt_mov(m) for m in sorted(movs, key=lambda x: x.get("fecha",""))],
    })


@app.route("/api/alertas")
@login_required
def api_alertas():
    """Devuelve los meses con margen por debajo del umbral configurado."""
    umbral = float(request.args.get("umbral", 15))  # % por defecto
    data   = cargar_datos()
    alertas = []
    for mes in data.get("meses", []):
        ing = sum(v for v in (mes.get("ingresos") or {}).values() if isinstance(v, (int, float)))
        egr = sum(v for v in (mes.get("egresos")  or {}).values() if isinstance(v, (int, float)))
        if ing <= 0:
            continue
        margen_pct = (ing - egr) / ing * 100
        if margen_pct < umbral:
            alertas.append({
                "periodo":    mes["periodo"],
                "facturacion": round(ing, 2),
                "egresos":     round(egr, 2),
                "margen_pct":  round(margen_pct, 2),
            })
    alertas.sort(key=lambda x: x["periodo"])
    return jsonify({"ok": True, "umbral": umbral, "alertas": alertas})


@app.route("/api/enviar_alerta", methods=["POST"])
@login_required
def api_enviar_alerta():
    """Envía un mail de resumen/alerta del último mes a la dirección configurada."""
    import smtplib
    from email.mime.text import MIMEText

    body_req = request.get_json() or {}
    periodo  = body_req.get("periodo")        # YYYY-MM opcional; si no se da, usa el último
    destino  = body_req.get("destino", "sgomezabuin@gmail.com")

    data  = cargar_datos()
    meses = sorted(data.get("meses", []), key=lambda m: m["periodo"])
    if not meses:
        return jsonify({"ok": False, "error": "No hay datos cargados"}), 400

    mes = next((m for m in meses if m["periodo"] == periodo), None) if periodo else meses[-1]
    if not mes:
        return jsonify({"ok": False, "error": f"Mes {periodo} no encontrado"}), 404

    ing = sum(v for v in (mes.get("ingresos") or {}).values() if isinstance(v, (int, float)))
    egr = sum(v for v in (mes.get("egresos")  or {}).values() if isinstance(v, (int, float)))
    margen     = ing - egr
    margen_pct = margen / ing * 100 if ing > 0 else 0
    retiro     = mes.get("retiro_socio", 0) or 0
    tc         = mes.get("tipo_cambio", {}) or {}

    cons  = (mes.get("detalles") or {}).get("consorcios",  [])[:3]
    provs = (mes.get("detalles") or {}).get("proveedores", [])[:3]

    def fmt(v): return f"${v:,.0f}".replace(",", ".")

    meses_es = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
    anio, mm = mes["periodo"].split("-")
    mes_label = f"{meses_es[int(mm)-1]} {anio}"

    alerta_flag = margen_pct < 15
    asunto = (f"⚠️ [AGVSA] ALERTA MARGEN — {mes_label}: {margen_pct:.1f}%"
              if alerta_flag else
              f"[AGVSA] Resumen {mes_label} — Facturación: {fmt(ing)} | Margen: {margen_pct:.1f}%")

    cons_html  = "".join(f"<li>{c['nombre']} — {fmt(c['monto'])}</li>" for c in cons)  if cons  else "<li>Sin datos</li>"
    provs_html = "".join(f"<li>{p['nombre']} — {fmt(p['monto'])}</li>" for p in provs) if provs else "<li>Sin datos</li>"
    tc_html    = f"Blue ${tc.get('blue',0):,.0f} | Oficial ${tc.get('oficial',0):,.0f}".replace(",",".") if tc else "No registrado"

    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;color:#1e293b">
      <div style="background:#1e3a5f;color:white;padding:18px 24px;border-radius:8px 8px 0 0">
        <h2 style="margin:0;font-size:18px">📊 AGVSA — {mes_label}</h2>
        {"<p style='margin:6px 0 0;color:#fca5a5;font-weight:bold'>⚠️ MARGEN POR DEBAJO DEL UMBRAL (15%)</p>" if alerta_flag else ""}
      </div>
      <div style="background:#f8fafc;padding:20px 24px;border:1px solid #e2e8f0;border-top:none">
        <table style="width:100%;border-collapse:collapse;font-size:14px">
          <tr><td style="padding:6px 0;color:#64748b">💰 Facturación</td><td style="text-align:right;font-weight:600">{fmt(ing)}</td></tr>
          <tr><td style="padding:6px 0;color:#64748b">💸 Egresos</td><td style="text-align:right;font-weight:600">{fmt(egr)}</td></tr>
          <tr style="border-top:2px solid #e2e8f0">
            <td style="padding:8px 0;font-weight:700">📈 Margen bruto</td>
            <td style="text-align:right;font-weight:700;color:{'#dc2626' if alerta_flag else '#15803d'}">{fmt(margen)} ({margen_pct:.1f}%)</td>
          </tr>
          {"<tr><td style='padding:6px 0;color:#64748b'>👤 Retiro socio</td><td style='text-align:right'>" + fmt(retiro) + "</td></tr>" if retiro else ""}
          <tr><td style="padding:6px 0;color:#64748b">💵 Tipo de cambio</td><td style="text-align:right;font-size:12px">{tc_html}</td></tr>
        </table>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
          <div>
            <p style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;margin:0 0 8px">🏢 Top Consorcios</p>
            <ol style="margin:0;padding-left:18px;font-size:13px">{cons_html}</ol>
          </div>
          <div>
            <p style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;margin:0 0 8px">🏭 Top Proveedores</p>
            <ol style="margin:0;padding-left:18px;font-size:13px">{provs_html}</ol>
          </div>
        </div>
      </div>
      <div style="background:#f1f5f9;padding:10px 24px;border-radius:0 0 8px 8px;font-size:11px;color:#94a3b8;text-align:center">
        Dashboard: <a href="http://127.0.0.1:5000" style="color:#2563eb">http://127.0.0.1:5000</a> · Generado automáticamente
      </div>
    </div>"""

    # Intentar envío por Gmail SMTP (si hay credenciales), si no devuelve el HTML para copiar
    gmail_user = os.environ.get("GMAIL_USER", "sgomezabuin@gmail.com")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")

    if gmail_pass:
        try:
            msg = MIMEText(html_body, "html", "utf-8")
            msg["Subject"] = asunto
            msg["From"]    = gmail_user
            msg["To"]      = destino
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
                s.login(gmail_user, gmail_pass)
                s.sendmail(gmail_user, [destino], msg.as_string())
            return jsonify({"ok": True, "enviado": True, "asunto": asunto})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "html": html_body})
    else:
        # Sin credenciales SMTP: devuelve el HTML para que lo use el frontend
        return jsonify({"ok": True, "enviado": False,
                        "asunto": asunto, "html": html_body,
                        "nota": "Configurá GMAIL_APP_PASSWORD para envío automático"})


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
