import streamlit as st
import pandas as pd
import boto3
import io
import copy
from datetime import date, datetime, timedelta
import calendar

st.set_page_config(page_title="Planning RH", page_icon="📋", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
[data-testid="stSidebar"] { background: #0f1117; border-right: 1px solid #1e2130; }
[data-testid="stSidebar"] * { color: #e0e4f0 !important; }
.stApp { background: #f5f6fa; }
.card { background: #fff; border-radius: 12px; padding: 24px; margin-bottom: 16px; border: 1px solid #e8eaf0; box-shadow: 0 2px 8px rgba(0,0,0,0.04); }
.kpi-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
.kpi { flex:1; min-width:120px; background:#fff; border-radius:10px; padding:16px 20px; border:1px solid #e8eaf0; box-shadow:0 1px 4px rgba(0,0,0,0.04); }
.kpi-label { font-size:11px; font-weight:600; letter-spacing:.08em; text-transform:uppercase; color:#8890a8; margin-bottom:6px; }
.kpi-value { font-size:28px; font-weight:600; font-family:'DM Mono',monospace; color:#1a1d2e; }
.badge { display:inline-block; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:500; font-family:'DM Mono',monospace; }
.badge-avenir  { background:#e8f4fd; color:#1a6fa8; }
.badge-encours { background:#e6f9f0; color:#177049; }
.badge-retard  { background:#fde8e8; color:#b81c1c; }
.badge-fait    { background:#f0f0f0; color:#555; }
.section-title { font-size:13px; font-weight:600; letter-spacing:.06em; text-transform:uppercase; color:#8890a8; margin-bottom:14px; padding-bottom:8px; border-bottom:1px solid #e8eaf0; }
.sidebar-sep { border-top:1px solid #2a2f45; margin:12px 0; }
.sidebar-section { font-size:10px; font-weight:600; letter-spacing:.1em; text-transform:uppercase; color:#4a5070 !important; padding:4px 0 2px; }
.stButton > button { border-radius:8px; font-weight:600; font-family:'DM Sans',sans-serif; }
/* Séparateurs dans le menu radio : masque le bouton radio, style en titre */
[data-testid="stSidebar"] div[data-testid="stRadio"] > div > label:has(div p) {
    cursor: default;
}
[data-testid="stSidebar"] div[data-testid="stRadio"] > div > label p {
    font-size: 13px !important;
}

.planning-table { width:100%; border-collapse:collapse; font-size:13px; }
.planning-table th { background:#0f1117; color:#e0e4f0; padding:10px 14px; text-align:left; font-weight:500; font-size:12px; letter-spacing:.04em; white-space:nowrap; }
.planning-table td { padding:10px 14px; border-bottom:1px solid #f0f1f5; vertical-align:middle; }
.planning-table tr:hover td { background:#f8f9fc; }
</style>
""", unsafe_allow_html=True)

# ─── CONSTANTES ────────────────────────────────────────────────────────────────
TYPES        = ["Paie", "Fiscal", "Administratif RH"]
FREQUENCES   = ["Mensuelle", "Annuelle", "Ponctuelle"]
STATUTS      = ["À venir", "En cours", "Fait", "En retard"]
REGLES_DL    = ["Fin de mois", "M+1", "Date fixe", "J+X"]
RESPONSABLES = ["RH", "Compta", "Direction"]
PRIORITES    = ["Basse", "Moyenne", "Haute", "Critique"]
TYPES_CONTRAT   = ["CDI", "CDD", "Intérim", "Alternance", "Stage", "Freelance", "Autre"]
STATUTS_CONTRAT = ["Actif", "À venir", "Terminé", "Suspendu"]

ACTIONS_COLS = ["id","nom_action","type","frequence","date_debut","duree","regle_deadline",
                "jour_deadline","mois_specifique","responsable","priorite","actif","nom_ressource","id_ressource"]
GENERATEUR_COLS = ["id_action","nom_action","date_occurrence","date_debut","date_fin","deadline",
                   "statut","responsable","type","nom_ressource","id_ressource","date_traitement","id_contrat"]
CONTRATS_COLS = ["id_contrat","id_ressource","nom_ressource","type_contrat",
                 "date_debut","date_fin","date_fin_essai","statut_contrat","notes"]

ACTIONS_CONTRAT_DEFAULT = {
    "CDI": [
        {"nom":"Signature contrat",        "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Remise équipement",         "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Visite médicale embauche",  "type":"Administratif RH","duree":1,"regle":"M+1","jour":30},
        {"nom":"Déclaration DPAE",          "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Inscription mutuelle",      "type":"Administratif RH","duree":3,"regle":"M+1","jour":15},
        {"nom":"Fin période d'essai",       "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Entretien période d'essai", "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Paie du mois",              "type":"Paie",            "duree":3,"regle":"Fin de mois","jour":30},
        {"nom":"Solde de tout compte",      "type":"Paie",            "duree":2,"regle":"M+1","jour":5},
        {"nom":"Attestation employeur",     "type":"Administratif RH","duree":1,"regle":"M+1","jour":5},
    ],
    "CDD": [
        {"nom":"Signature contrat",        "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Déclaration DPAE",         "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Visite médicale embauche", "type":"Administratif RH","duree":1,"regle":"M+1","jour":30},
        {"nom":"Inscription mutuelle",     "type":"Administratif RH","duree":3,"regle":"M+1","jour":15},
        {"nom":"Fin période d'essai",      "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Paie du mois",             "type":"Paie",            "duree":3,"regle":"Fin de mois","jour":30},
        {"nom":"Renouvellement CDD",       "type":"Administratif RH","duree":2,"regle":"Date fixe","jour":1},
        {"nom":"Solde de tout compte",     "type":"Paie",            "duree":2,"regle":"M+1","jour":5},
        {"nom":"Attestation employeur",    "type":"Administratif RH","duree":1,"regle":"M+1","jour":5},
        {"nom":"Certificat de travail",    "type":"Administratif RH","duree":1,"regle":"M+1","jour":5},
    ],
    "Alternance": [
        {"nom":"Signature contrat",    "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Déclaration DPAE",     "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Enregistrement OPCO",  "type":"Administratif RH","duree":5,"regle":"M+1","jour":15},
        {"nom":"Visite médicale",      "type":"Administratif RH","duree":1,"regle":"M+1","jour":30},
        {"nom":"Paie du mois",         "type":"Paie",            "duree":3,"regle":"Fin de mois","jour":30},
        {"nom":"Bilan mi-parcours",    "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Fin de contrat",       "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
    ],
    "Stage": [
        {"nom":"Convention de stage",           "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Déclaration accueil stagiaire",  "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Gratification mensuelle",        "type":"Paie",            "duree":2,"regle":"Fin de mois","jour":30},
        {"nom":"Bilan mi-stage",                 "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Attestation de stage",           "type":"Administratif RH","duree":1,"regle":"M+1","jour":5},
    ],
    "Intérim": [
        {"nom":"Contrat de mission","type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Déclaration DPAE",  "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Paie du mois",      "type":"Paie",            "duree":3,"regle":"Fin de mois","jour":30},
        {"nom":"Fin de mission",    "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
    ],
    "Freelance": [
        {"nom":"Signature contrat prestation","type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Facture mensuelle",            "type":"Fiscal",          "duree":2,"regle":"Fin de mois","jour":30},
        {"nom":"Fin de mission",               "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
    ],
    "Autre": [
        {"nom":"Signature contrat","type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Déclaration DPAE", "type":"Administratif RH","duree":1,"regle":"Date fixe","jour":1},
        {"nom":"Paie du mois",     "type":"Paie",            "duree":3,"regle":"Fin de mois","jour":30},
    ],
}

# ─── R2 ────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_r2_client():
    return boto3.client("s3",
        endpoint_url=f"https://{st.secrets['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=st.secrets["R2_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["R2_SECRET_KEY"],
        region_name="auto")

def load_parquet(key, cols):
    try:
        obj = get_r2_client().get_object(Bucket=st.secrets["R2_BUCKET"], Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        return pd.DataFrame(columns=cols)

def save_parquet(df, key):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    get_r2_client().put_object(Bucket=st.secrets["R2_BUCKET"], Key=key, Body=buf.getvalue())

# ─── BUSINESS LOGIC ────────────────────────────────────────────────────────────
def compute_deadline(action, occ_date):
    regle = action["regle_deadline"]
    jour  = int(action["jour_deadline"] or 1)
    if regle == "Fin de mois":
        return occ_date.replace(day=calendar.monthrange(occ_date.year, occ_date.month)[1])
    elif regle == "M+1":
        nm = (occ_date.replace(day=1) + timedelta(days=32)).replace(day=1)
        return nm.replace(day=min(jour, calendar.monthrange(nm.year, nm.month)[1]))
    elif regle == "Date fixe":
        m = int(action.get("mois_specifique") or occ_date.month)
        return occ_date.replace(month=m, day=min(jour, calendar.monthrange(occ_date.year, m)[1]))
    elif regle == "J+X":
        return occ_date + timedelta(days=jour)
    return occ_date

def compute_statut(date_debut, deadline, date_traitement):
    today = date.today()
    if pd.notna(date_traitement) and date_traitement: return "Fait"
    if today > deadline: return "En retard"
    if today >= date_debut: return "En cours"
    return "À venir"

def generate_occurrences(actions_df):
    rows = []
    for _, action in actions_df.iterrows():
        if str(action.get("actif","Oui")).lower() != "oui": continue
        freq  = action.get("frequence","Mensuelle")
        debut = pd.to_datetime(action["date_debut"]).date()
        n     = 12 if freq == "Mensuelle" else 1
        for i in range(n):
            month = debut.month + i
            year  = debut.year + (month-1)//12
            month = ((month-1)%12)+1
            try: occ = debut.replace(year=year, month=month)
            except ValueError: occ = date(year, month, calendar.monthrange(year, month)[1])
            df_fin = occ + timedelta(days=int(action.get("duree",1)))
            dl     = compute_deadline(action.to_dict(), occ)
            rows.append({
                "id_action":       action["id"],        "nom_action":    action["nom_action"],
                "date_occurrence": occ,                 "date_debut":    occ,
                "date_fin":        df_fin,              "deadline":      dl,
                "statut":          compute_statut(occ, dl, action.get("date_traitement")),
                "responsable":     action.get("responsable",""),
                "type":            action.get("type",""),
                "nom_ressource":   action.get("nom_ressource",""),
                "id_ressource":    action.get("id_ressource", None),
                "date_traitement": None,                "id_contrat":    None,
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=GENERATEUR_COLS)

# ─── SESSION STATE ─────────────────────────────────────────────────────────────
for k in ["actions_df","gen_df","contrats_df","actions_contrat_custom"]:
    if k not in st.session_state:
        st.session_state[k] = None

def load_data():
    st.session_state.actions_df          = load_parquet("planning_rh/actions.parquet",   ACTIONS_COLS)
    st.session_state.gen_df              = load_parquet("planning_rh/generateur.parquet", GENERATEUR_COLS)
    st.session_state.contrats_df         = load_parquet("planning_rh/contrats.parquet",   CONTRATS_COLS)
    custom = load_parquet("planning_rh/actions_contrat_custom.parquet", ["type_contrat","actions_json"])
    if custom.empty:
        st.session_state.actions_contrat_custom = copy.deepcopy(ACTIONS_CONTRAT_DEFAULT)
    else:
        import json
        st.session_state.actions_contrat_custom = {
            row["type_contrat"]: json.loads(row["actions_json"])
            for _, row in custom.iterrows()
        }
    for df, key in [
        (st.session_state.actions_df,  "planning_rh/actions.parquet"),
        (st.session_state.gen_df,      "planning_rh/generateur.parquet"),
        (st.session_state.contrats_df, "planning_rh/contrats.parquet"),
    ]:
        if df.empty:
            try: save_parquet(df, key)
            except: pass

def reload_and_regen():
    gen = generate_occurrences(st.session_state.actions_df)
    st.session_state.gen_df = gen
    save_parquet(st.session_state.actions_df,  "planning_rh/actions.parquet")
    save_parquet(gen,                          "planning_rh/generateur.parquet")

def save_custom_actions():
    import json
    rows = [{"type_contrat": k, "actions_json": json.dumps(v)}
            for k, v in st.session_state.actions_contrat_custom.items()]
    df = pd.DataFrame(rows)
    save_parquet(df, "planning_rh/actions_contrat_custom.parquet")

if st.session_state.actions_df is None:
    load_data()

actions_df  = st.session_state.actions_df
gen_df      = st.session_state.gen_df
contrats_df = st.session_state.contrats_df if st.session_state.contrats_df is not None else pd.DataFrame(columns=CONTRATS_COLS)
ACTIONS_CONTRAT = st.session_state.actions_contrat_custom or ACTIONS_CONTRAT_DEFAULT

# ─── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📋 Planning RH")
    st.markdown("---")

    page_active = st.radio(
        "Navigation",
        [
            "🏠 Tableau de bord",
            "── Contrats ──",
            "📄 Nouveau contrat",
            "📋 Gérer les contrats",
            "── Actions ──",
            "➕ Nouvelle action",
            "✏️ Gérer les actions",
            "✅ Actions terminées",
            "── Planning ──",
            "📅 Planning",
            "── Paramètres ──",
            "⚙️ Paramétrage types de contrats",
        ],
        label_visibility="collapsed",
    )

    st.markdown("---")
    if st.button("🔄 Recharger", use_container_width=True):
        load_data()
        st.success("Données rechargées.")

# Les séparateurs ne sont pas des pages — on redirige vers la première vraie page suivante
SEPARATEURS = {"── Contrats ──", "── Actions ──", "── Planning ──", "── Paramètres ──"}
if page_active in SEPARATEURS:
    page_active = "🏠 Tableau de bord"

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: TABLEAU DE BORD
# ══════════════════════════════════════════════════════════════════════════════
if page_active == "🏠 Tableau de bord":
    st.markdown("# Tableau de bord")
    if gen_df is None or gen_df.empty:
        st.info("Aucune donnée. Commencez par créer des actions ou des contrats.")
    else:
        gen = gen_df.copy()
        for col in ["date_debut","deadline","date_occurrence"]:
            if col in gen.columns:
                gen[col] = pd.to_datetime(gen[col], errors="coerce").dt.date
        today     = date.today()
        total     = len(gen[gen["statut"] != "Fait"])
        en_retard = (gen["statut"]=="En retard").sum()
        en_cours  = (gen["statut"]=="En cours").sum()
        a_venir   = (gen["statut"]=="À venir").sum()
        fait      = (gen["statut"]=="Fait").sum()
        nb_contrats = len(contrats_df) if not contrats_df.empty else 0

        st.markdown(f"""<div class="kpi-row">
            <div class="kpi"><div class="kpi-label">Contrats actifs</div><div class="kpi-value">{nb_contrats}</div></div>
            <div class="kpi"><div class="kpi-label">Actions en cours</div><div class="kpi-value" style="color:#177049">{en_cours}</div></div>
            <div class="kpi"><div class="kpi-label">En retard</div><div class="kpi-value" style="color:#e53935">{en_retard}</div></div>
            <div class="kpi"><div class="kpi-label">À venir</div><div class="kpi-value" style="color:#1a6fa8">{a_venir}</div></div>
            <div class="kpi"><div class="kpi-label">Terminées</div><div class="kpi-value" style="color:#8890a8">{fait}</div></div>
        </div>""", unsafe_allow_html=True)

        st.markdown('<div class="section-title">⚠️ Actions urgentes (deadline ≤ 7 jours)</div>', unsafe_allow_html=True)
        urgentes = gen[(gen["statut"].isin(["En retard","En cours"])) & (gen["deadline"] <= today+timedelta(days=7))].sort_values("deadline")
        if urgentes.empty:
            st.success("Aucune action urgente 🎉")
        else:
            bmap = {"À venir":("badge-avenir","À venir"),"En cours":("badge-encours","En cours"),"En retard":("badge-retard","En retard"),"Fait":("badge-fait","Fait")}
            rh = "".join(f'<tr><td><b>{r["nom_action"]}</b></td><td>{r["type"]}</td><td>{r["responsable"]}</td>'
                         f'<td>{r["deadline"].strftime("%d/%m/%Y") if pd.notna(r["deadline"]) else "—"}</td>'
                         f'<td><span class="badge {bmap.get(r["statut"],("badge-avenir",""))[0]}">{r["statut"]}</span></td></tr>'
                         for _, r in urgentes.iterrows())
            st.markdown(f'<div class="card" style="padding:0;overflow:hidden"><table class="planning-table"><thead><tr><th>Action</th><th>Type</th><th>Responsable</th><th>Deadline</th><th>Statut</th></tr></thead><tbody>{rh}</tbody></table></div>', unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-title">Par type</div>', unsafe_allow_html=True)
            st.dataframe(gen[gen["statut"]!="Fait"].groupby("type")["statut"].value_counts().unstack(fill_value=0), use_container_width=True)
        with col2:
            st.markdown('<div class="section-title">Par responsable</div>', unsafe_allow_html=True)
            st.dataframe(gen[gen["statut"]!="Fait"].groupby("responsable")["statut"].value_counts().unstack(fill_value=0), use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: NOUVEAU CONTRAT
# ══════════════════════════════════════════════════════════════════════════════
elif page_active == "📄 Nouveau contrat":
    st.markdown("# Nouveau contrat")
    next_cid = 1 if contrats_df.empty or "id_contrat" not in contrats_df.columns else int(contrats_df["id_contrat"].max())+1
    st.markdown(f'<div class="card"><span style="font-size:12px;color:#8890a8;font-family:\'DM Mono\',monospace">ID contrat auto-assigné : <b>#{next_cid}</b></span></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Étape 1 — Informations du contrat</div>', unsafe_allow_html=True)
    with st.form("form_contrat", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1: id_ressource  = st.text_input("ID Ressource *", placeholder="Ex: EMP-001")
        with c2: nom_ressource = st.text_input("Nom complet *",  placeholder="Ex: Jean Dupont")

        c1, c2, c3 = st.columns(3)
        with c1: type_contrat   = st.selectbox("Type de contrat *", TYPES_CONTRAT)
        with c2: statut_contrat = st.selectbox("Statut", STATUTS_CONTRAT)
        with c3: notes          = st.text_input("Notes", placeholder="Informations complémentaires")

        c1, c2, c3 = st.columns(3)
        with c1: date_debut_c   = st.date_input("Date de début *", value=date.today())
        with c2: date_fin_essai = st.date_input("Fin période d'essai", value=date.today()+timedelta(days=90))
        with c3: has_date_fin   = st.checkbox("Date de fin prévue ?", value=False)
        date_fin_c = None
        if has_date_fin:
            date_fin_c = st.date_input("Date de fin de contrat", value=date.today()+timedelta(days=365))

        valider = st.form_submit_button("Suivant — Choisir les actions ▶", use_container_width=True, type="primary")

    if valider:
        if not id_ressource or not nom_ressource:
            st.error("L'ID et le nom de la ressource sont obligatoires.")
        else:
            # Initialise le draft avec les actions par défaut (copie profonde pour édition)
            st.session_state["contrat_draft"] = {
                "id_contrat": next_cid, "id_ressource": id_ressource,
                "nom_ressource": nom_ressource, "type_contrat": type_contrat,
                "date_debut": date_debut_c, "date_fin": date_fin_c,
                "date_fin_essai": date_fin_essai, "statut_contrat": statut_contrat, "notes": notes,
            }
            st.session_state["contrat_actions_edit"] = copy.deepcopy(ACTIONS_CONTRAT.get(type_contrat, []))
            st.rerun()

    # ── Étape 2 ──────────────────────────────────────────────────────────────
    if "contrat_draft" in st.session_state:
        draft  = st.session_state["contrat_draft"]
        edit_actions = st.session_state.get("contrat_actions_edit", [])

        st.markdown("---")
        st.markdown(f'<div class="section-title">Étape 2 — Actions proposées · {draft["type_contrat"]} · {draft["nom_ressource"]}</div>', unsafe_allow_html=True)
        st.caption("Cochez les actions à créer. Cliquez ✏️ pour modifier une ligne avant de valider.")

        # En-têtes
        h = st.columns([0.3, 0.3, 2.5, 1.8, 0.8, 1.8, 0.8, 0.5])
        for col, lbl in zip(h, ["✓","#","Nom","Type","Durée","Règle","Jour","Edit"]):
            col.markdown(f'<div style="font-size:11px;font-weight:600;color:#8890a8;text-transform:uppercase;padding-bottom:4px;border-bottom:1px solid #e8eaf0">{lbl}</div>', unsafe_allow_html=True)

        checks = {}
        edit_idx = st.session_state.get("edit_action_idx", None)

        for idx, act in enumerate(edit_actions):
            c0,c1,c2,c3,c4,c5,c6,c7 = st.columns([0.3,0.3,2.5,1.8,0.8,1.8,0.8,0.5])
            with c0: checks[idx] = st.checkbox("", value=True, key=f"chk_{idx}", label_visibility="collapsed")
            with c1: st.markdown(f'<div style="padding:6px 0;font-size:12px;color:#8890a8">{idx+1}</div>', unsafe_allow_html=True)
            with c2: st.markdown(f'<div style="padding:6px 0;font-size:13px;font-weight:500">{act["nom"]}</div>', unsafe_allow_html=True)
            with c3: st.markdown(f'<div style="padding:6px 0;font-size:12px;color:#8890a8">{act["type"]}</div>', unsafe_allow_html=True)
            with c4: st.markdown(f'<div style="padding:6px 0;font-size:13px">{act["duree"]}j</div>', unsafe_allow_html=True)
            with c5: st.markdown(f'<div style="padding:6px 0;font-size:12px">{act["regle"]}</div>', unsafe_allow_html=True)
            with c6: st.markdown(f'<div style="padding:6px 0;font-size:13px">{act["jour"]}</div>', unsafe_allow_html=True)
            with c7:
                if st.button("✏️", key=f"edit_act_btn_{idx}", help="Modifier cette action"):
                    st.session_state["edit_action_idx"] = idx
                    st.rerun()

        # Formulaire d'édition inline
        if edit_idx is not None and edit_idx < len(edit_actions):
            act_to_edit = edit_actions[edit_idx]
            st.markdown(f'<div class="card" style="margin-top:8px;border-left:3px solid #378add"><b>Modifier l\'action #{edit_idx+1}</b></div>', unsafe_allow_html=True)
            with st.form("edit_action_inline"):
                ec1, ec2, ec3, ec4, ec5 = st.columns([3, 2, 1, 2, 1])
                with ec1: new_nom   = st.text_input("Nom", value=act_to_edit["nom"])
                with ec2: new_type  = st.selectbox("Type", TYPES, index=TYPES.index(act_to_edit["type"]) if act_to_edit["type"] in TYPES else 0)
                with ec3: new_duree = st.number_input("Durée", min_value=1, value=int(act_to_edit["duree"]))
                with ec4: new_regle = st.selectbox("Règle", REGLES_DL, index=REGLES_DL.index(act_to_edit["regle"]) if act_to_edit["regle"] in REGLES_DL else 0)
                with ec5: new_jour  = st.number_input("Jour", min_value=1, max_value=31, value=int(act_to_edit["jour"]))
                sc1, sc2 = st.columns(2)
                with sc1:
                    if st.form_submit_button("💾 Enregistrer", type="primary", use_container_width=True):
                        st.session_state["contrat_actions_edit"][edit_idx] = {"nom": new_nom, "type": new_type, "duree": new_duree, "regle": new_regle, "jour": new_jour}
                        st.session_state["edit_action_idx"] = None
                        st.rerun()
                with sc2:
                    if st.form_submit_button("✖ Annuler", use_container_width=True):
                        st.session_state["edit_action_idx"] = None
                        st.rerun()

        selected = [edit_actions[i] for i, c in checks.items() if c]
        st.markdown(f'<div style="margin:8px 0;font-size:13px;color:#8890a8"><b>{len(selected)}</b> action(s) sélectionnée(s)</div>', unsafe_allow_html=True)

        cc1, cc2 = st.columns(2)
        with cc1:
            if st.button("✅ Créer le contrat et les actions", type="primary", use_container_width=True):
                if not selected:
                    st.warning("Sélectionnez au moins une action.")
                else:
                    new_c = {
                        "id_contrat": draft["id_contrat"], "id_ressource": draft["id_ressource"],
                        "nom_ressource": draft["nom_ressource"], "type_contrat": draft["type_contrat"],
                        "date_debut": pd.Timestamp(draft["date_debut"]),
                        "date_fin": pd.Timestamp(draft["date_fin"]) if draft["date_fin"] else None,
                        "date_fin_essai": pd.Timestamp(draft["date_fin_essai"]),
                        "statut_contrat": draft["statut_contrat"], "notes": draft["notes"],
                    }
                    st.session_state.contrats_df = pd.concat([contrats_df, pd.DataFrame([new_c])], ignore_index=True)
                    naid = 1 if actions_df.empty or "id" not in actions_df.columns else int(actions_df["id"].max())+1
                    new_acts = [{"id": naid+i, "nom_action": a["nom"]+" — "+draft["nom_ressource"],
                                 "type": a["type"], "frequence": "Ponctuelle",
                                 "date_debut": pd.Timestamp(draft["date_debut"]),
                                 "duree": a["duree"], "regle_deadline": a["regle"],
                                 "jour_deadline": a["jour"], "mois_specifique": None,
                                 "responsable": "RH", "priorite": "Haute", "actif": "Oui",
                                 "nom_ressource": draft["nom_ressource"], "id_ressource": draft["id_ressource"]}
                                for i, a in enumerate(selected)]
                    st.session_state.actions_df = pd.concat([actions_df, pd.DataFrame(new_acts)], ignore_index=True)
                    try:
                        gen_new = generate_occurrences(st.session_state.actions_df)
                        new_ids = [a["id"] for a in new_acts]
                        gen_new.loc[gen_new["id_action"].isin(new_ids), "id_contrat"] = draft["id_contrat"]
                        st.session_state.gen_df = gen_new
                        save_parquet(st.session_state.actions_df,  "planning_rh/actions.parquet")
                        save_parquet(st.session_state.gen_df,       "planning_rh/generateur.parquet")
                        save_parquet(st.session_state.contrats_df,  "planning_rh/contrats.parquet")
                        for k in ["contrat_draft","contrat_actions_edit","edit_action_idx"]:
                            st.session_state.pop(k, None)
                        st.success(f"✅ Contrat #{draft['id_contrat']} créé avec {len(selected)} action(s) !")
                        st.balloons(); st.rerun()
                    except Exception as e:
                        st.error(f"Erreur R2 : {e}")
        with cc2:
            if st.button("✖ Annuler", use_container_width=True):
                for k in ["contrat_draft","contrat_actions_edit","edit_action_idx"]:
                    st.session_state.pop(k, None)
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: GÉRER LES CONTRATS
# ══════════════════════════════════════════════════════════════════════════════
elif page_active == "📋 Gérer les contrats":
    st.markdown("# Gérer les contrats")
    if contrats_df.empty:
        st.info("Aucun contrat enregistré.")
    else:
        disp = contrats_df.copy()
        for col in ["date_debut","date_fin","date_fin_essai"]:
            if col in disp.columns:
                disp[col] = pd.to_datetime(disp[col], errors="coerce").dt.strftime("%d/%m/%Y")
        st.dataframe(disp, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("### Modifier un contrat")
        cids = contrats_df["id_contrat"].tolist()
        sel_cid = st.selectbox("Sélectionner un contrat", cids,
            format_func=lambda i: f"#{i} — {contrats_df[contrats_df['id_contrat']==i]['nom_ressource'].values[0]} ({contrats_df[contrats_df['id_contrat']==i]['type_contrat'].values[0]})")
        crow = contrats_df[contrats_df["id_contrat"]==sel_cid].iloc[0]

        tab_edit, tab_del = st.tabs(["✏️ Modifier","🗑️ Supprimer"])
        with tab_edit:
            with st.form("edit_contrat"):
                c1, c2 = st.columns(2)
                with c1: edit_id_r  = st.text_input("ID Ressource", value=str(crow.get("id_ressource","")))
                with c2: edit_nom_r = st.text_input("Nom ressource", value=str(crow.get("nom_ressource","")))
                c1, c2, c3 = st.columns(3)
                with c1: edit_tc  = st.selectbox("Type contrat", TYPES_CONTRAT, index=TYPES_CONTRAT.index(crow["type_contrat"]) if crow["type_contrat"] in TYPES_CONTRAT else 0)
                with c2: edit_sc  = st.selectbox("Statut", STATUTS_CONTRAT, index=STATUTS_CONTRAT.index(crow["statut_contrat"]) if crow["statut_contrat"] in STATUTS_CONTRAT else 0)
                with c3: edit_note= st.text_input("Notes", value=str(crow.get("notes","")))
                c1, c2, c3 = st.columns(3)
                with c1: edit_dd  = st.date_input("Date début", value=pd.to_datetime(crow["date_debut"]).date())
                with c2: edit_fe  = st.date_input("Fin essai",  value=pd.to_datetime(crow["date_fin_essai"]).date() if pd.notna(crow.get("date_fin_essai")) else date.today())
                with c3:
                    has_fin = pd.notna(crow.get("date_fin"))
                    edit_hf = st.checkbox("Date fin prévue", value=has_fin)
                    edit_df = st.date_input("Date fin", value=pd.to_datetime(crow["date_fin"]).date() if has_fin else date.today()) if edit_hf else None

                if st.form_submit_button("💾 Sauvegarder", use_container_width=True, type="primary"):
                    idx = st.session_state.contrats_df[st.session_state.contrats_df["id_contrat"]==sel_cid].index[0]
                    st.session_state.contrats_df.at[idx,"id_ressource"]  = edit_id_r
                    st.session_state.contrats_df.at[idx,"nom_ressource"] = edit_nom_r
                    st.session_state.contrats_df.at[idx,"type_contrat"]  = edit_tc
                    st.session_state.contrats_df.at[idx,"statut_contrat"]= edit_sc
                    st.session_state.contrats_df.at[idx,"notes"]         = edit_note
                    st.session_state.contrats_df.at[idx,"date_debut"]    = pd.Timestamp(edit_dd)
                    st.session_state.contrats_df.at[idx,"date_fin_essai"]= pd.Timestamp(edit_fe)
                    st.session_state.contrats_df.at[idx,"date_fin"]      = pd.Timestamp(edit_df) if edit_df else None
                    try:
                        save_parquet(st.session_state.contrats_df, "planning_rh/contrats.parquet")
                        st.success("✅ Contrat mis à jour."); st.rerun()
                    except Exception as e: st.error(f"Erreur R2 : {e}")

        with tab_del:
            st.warning(f"Supprimer définitivement le contrat #{sel_cid} — {crow['nom_ressource']} ?")
            if st.button("🗑️ Confirmer la suppression", type="primary"):
                st.session_state.contrats_df = st.session_state.contrats_df[st.session_state.contrats_df["id_contrat"]!=sel_cid].reset_index(drop=True)
                try:
                    save_parquet(st.session_state.contrats_df, "planning_rh/contrats.parquet")
                    st.success("Contrat supprimé."); st.rerun()
                except Exception as e: st.error(f"Erreur R2 : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: NOUVELLE ACTION
# ══════════════════════════════════════════════════════════════════════════════
elif page_active == "➕ Nouvelle action":
    st.markdown("# Nouvelle action")
    next_id = 1 if (actions_df.empty or "id" not in actions_df.columns) else int(actions_df["id"].max())+1
    st.markdown(f'<div class="card"><span style="font-size:12px;color:#8890a8;font-family:\'DM Mono\',monospace">ID auto-assigné : <b>#{next_id}</b></span></div>', unsafe_allow_html=True)
    with st.form("form_action", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1: nom_action    = st.text_input("Nom de l'action *", placeholder="Ex: Paie mensuelle")
        with c2: nom_ressource = st.text_input("Nom de la ressource", placeholder="Ex: Jean Dupont")
        c1, c2 = st.columns(2)
        with c1: id_ressource  = st.text_input("ID Ressource", placeholder="Ex: EMP-001")
        with c2: pass
        c1, c2, c3 = st.columns(3)
        with c1: type_action = st.selectbox("Type *", TYPES)
        with c2: frequence   = st.selectbox("Fréquence *", FREQUENCES)
        with c3: priorite    = st.selectbox("Priorité *", PRIORITES, index=2)
        c1, c2 = st.columns(2)
        with c1: date_debut  = st.date_input("Date de début *", value=date.today())
        with c2: duree       = st.number_input("Durée (jours) *", min_value=1, max_value=365, value=3)
        c1, c2, c3 = st.columns(3)
        with c1: regle_dl    = st.selectbox("Règle deadline *", REGLES_DL)
        with c2: jour_dl     = st.number_input("Jour deadline", min_value=1, max_value=31, value=30)
        with c3: mois_sp     = st.number_input("Mois spécifique", min_value=0, max_value=12, value=0)
        c1, c2 = st.columns(2)
        with c1: responsable = st.selectbox("Responsable *", RESPONSABLES)
        with c2: actif       = st.selectbox("Actif", ["Oui","Non"])
        submitted = st.form_submit_button("✅ Créer l'action", use_container_width=True, type="primary")
    if submitted:
        if not nom_action: st.error("Le nom est obligatoire.")
        else:
            new_row = {"id": next_id, "nom_action": nom_action, "type": type_action,
                       "frequence": frequence, "date_debut": pd.Timestamp(date_debut),
                       "duree": duree, "regle_deadline": regle_dl, "jour_deadline": jour_dl,
                       "mois_specifique": mois_sp if mois_sp>0 else None,
                       "responsable": responsable, "priorite": priorite, "actif": actif,
                       "nom_ressource": nom_ressource or None, "id_ressource": id_ressource or None}
            st.session_state.actions_df = pd.concat([actions_df, pd.DataFrame([new_row])], ignore_index=True)
            try: reload_and_regen(); st.success(f"✅ Action #{next_id} créée !"); st.balloons()
            except Exception as e: st.error(f"Erreur R2 : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: GÉRER LES ACTIONS
# ══════════════════════════════════════════════════════════════════════════════
elif page_active == "✏️ Gérer les actions":
    st.markdown("# Gérer les actions")
    active_df = actions_df[actions_df["actif"]=="Oui"] if not actions_df.empty else actions_df
    if active_df.empty:
        st.info("Aucune action active.")
    else:
        disp = active_df.copy()
        disp["date_debut"] = pd.to_datetime(disp["date_debut"]).dt.strftime("%d/%m/%Y")
        st.dataframe(disp, use_container_width=True, hide_index=True)
        st.markdown("---")
        sel_id = st.selectbox("Sélectionner une action", active_df["id"].tolist(),
            format_func=lambda i: f"#{i} — {active_df[active_df['id']==i]['nom_action'].values[0]}")
        row = active_df[active_df["id"]==sel_id].iloc[0]
        tab_edit, tab_del = st.tabs(["✏️ Modifier","🗑️ Supprimer"])
        with tab_edit:
            with st.form("edit_form"):
                c1, c2 = st.columns(2)
                with c1: nom_action    = st.text_input("Nom", value=row["nom_action"])
                with c2: nom_ressource = st.text_input("Nom ressource", value=str(row.get("nom_ressource") or ""))
                c1, c2 = st.columns(2)
                with c1: id_ressource  = st.text_input("ID Ressource", value=str(row.get("id_ressource") or ""))
                with c2: pass
                c1, c2, c3 = st.columns(3)
                with c1: type_action = st.selectbox("Type", TYPES, index=TYPES.index(row["type"]) if row["type"] in TYPES else 0)
                with c2: frequence   = st.selectbox("Fréquence", FREQUENCES, index=FREQUENCES.index(row["frequence"]) if row["frequence"] in FREQUENCES else 0)
                with c3: priorite    = st.selectbox("Priorité", PRIORITES, index=PRIORITES.index(row["priorite"]) if row["priorite"] in PRIORITES else 2)
                c1, c2 = st.columns(2)
                with c1: date_debut  = st.date_input("Date début", value=pd.to_datetime(row["date_debut"]).date())
                with c2: duree       = st.number_input("Durée", min_value=1, value=int(row["duree"]))
                c1, c2, c3 = st.columns(3)
                with c1: regle_dl   = st.selectbox("Règle deadline", REGLES_DL, index=REGLES_DL.index(row["regle_deadline"]) if row["regle_deadline"] in REGLES_DL else 0)
                with c2: jour_dl    = st.number_input("Jour deadline", min_value=1, max_value=31, value=int(row["jour_deadline"] or 1))
                with c3: mois_sp    = st.number_input("Mois spécifique", min_value=0, max_value=12, value=int(row.get("mois_specifique") or 0))
                c1, c2 = st.columns(2)
                with c1: responsable = st.selectbox("Responsable", RESPONSABLES, index=RESPONSABLES.index(row["responsable"]) if row["responsable"] in RESPONSABLES else 0)
                with c2: actif       = st.selectbox("Actif", ["Oui","Non"], index=0 if row["actif"]=="Oui" else 1)
                if st.form_submit_button("💾 Sauvegarder", use_container_width=True, type="primary"):
                    idx = st.session_state.actions_df[st.session_state.actions_df["id"]==sel_id].index[0]
                    for k,v in {"nom_action":nom_action,"type":type_action,"frequence":frequence,
                                "date_debut":pd.Timestamp(date_debut),"duree":duree,
                                "regle_deadline":regle_dl,"jour_deadline":jour_dl,
                                "mois_specifique":mois_sp if mois_sp>0 else None,
                                "responsable":responsable,"priorite":priorite,"actif":actif,
                                "nom_ressource":nom_ressource or None,"id_ressource":id_ressource or None}.items():
                        st.session_state.actions_df.at[idx,k]=v
                    try: reload_and_regen(); st.success("✅ Mis à jour."); st.rerun()
                    except Exception as e: st.error(f"Erreur R2 : {e}")
        with tab_del:
            st.warning(f"Archiver l'action #{sel_id} — {row['nom_action']} ?")
            if st.button("🗑️ Archiver (marquer inactif)", type="primary"):
                idx = st.session_state.actions_df[st.session_state.actions_df["id"]==sel_id].index[0]
                st.session_state.actions_df.at[idx,"actif"] = "Non"
                try: reload_and_regen(); st.success("Action archivée."); st.rerun()
                except Exception as e: st.error(f"Erreur R2 : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: ACTIONS TERMINÉES
# ══════════════════════════════════════════════════════════════════════════════
elif page_active == "✅ Actions terminées":
    st.markdown("# Actions terminées")
    if gen_df is None or gen_df.empty:
        st.info("Aucune donnée disponible.")
    else:
        gen = gen_df.copy()
        for col in ["date_debut","deadline","date_occurrence","date_traitement"]:
            if col in gen.columns:
                gen[col] = pd.to_datetime(gen[col], errors="coerce").dt.date
        terminees = gen[gen["statut"]=="Fait"].sort_values("date_traitement", ascending=False)
        if terminees.empty:
            st.info("Aucune action marquée comme terminée.")
        else:
            st.markdown(f'**{len(terminees)} action(s) terminée(s)**')
            # Filtres
            c1, c2, c3 = st.columns(3)
            with c1: f_type = st.multiselect("Type", terminees["type"].dropna().unique().tolist(), default=terminees["type"].dropna().unique().tolist())
            with c2: f_resp = st.multiselect("Responsable", terminees["responsable"].dropna().unique().tolist(), default=terminees["responsable"].dropna().unique().tolist())
            with c3: f_res  = st.multiselect("Ressource", terminees["nom_ressource"].dropna().unique().tolist() or ["—"], default=terminees["nom_ressource"].dropna().unique().tolist() or ["—"])

            filtered_t = terminees[terminees["type"].isin(f_type) & terminees["responsable"].isin(f_resp)]

            rh = ""
            for _, r in filtered_t.iterrows():
                occ = r["date_occurrence"].strftime("%d/%m/%Y") if pd.notna(r["date_occurrence"]) else "—"
                trmt = r["date_traitement"].strftime("%d/%m/%Y") if pd.notna(r["date_traitement"]) else "—"
                dl  = r["deadline"].strftime("%d/%m/%Y") if pd.notna(r["deadline"]) else "—"
                rh += (f'<tr><td style="font-family:monospace;font-size:11px;color:#8890a8">#{r["id_action"]}</td>'
                       f'<td><b>{r["nom_action"]}</b></td><td>{r["type"]}</td><td>{r["responsable"]}</td>'
                       f'<td>{occ}</td><td>{dl}</td>'
                       f'<td style="color:#177049;font-weight:600">{trmt}</td>'
                       f'<td><button-placeholder data-id="{r["id_action"]}" data-occ="{r["date_occurrence"]}">↩</button-placeholder></td></tr>')

            st.markdown(f'<div class="card" style="padding:0;overflow:hidden"><table class="planning-table"><thead><tr><th>ID</th><th>Action</th><th>Type</th><th>Responsable</th><th>Occurrence</th><th>Deadline</th><th>Traité le</th><th>Remettre</th></tr></thead><tbody>{rh}</tbody></table></div>', unsafe_allow_html=True)

            st.markdown("---")
            st.markdown("### Remettre une action en cours")
            options_t = filtered_t.apply(lambda r: f"#{r['id_action']} — {r['nom_action']} ({r['date_occurrence'].strftime('%m/%Y') if pd.notna(r['date_occurrence']) else '?'})", axis=1).tolist()
            if options_t:
                sel_t = st.selectbox("Sélectionner l'action à remettre en cours", options_t)
                if st.button("↩ Remettre en cours", type="primary"):
                    idx_t = options_t.index(sel_t)
                    target = filtered_t.iloc[idx_t]
                    mask = ((st.session_state.gen_df["id_action"]==target["id_action"]) &
                            (pd.to_datetime(st.session_state.gen_df["date_occurrence"]).dt.date==target["date_occurrence"]))
                    st.session_state.gen_df.loc[mask,"date_traitement"] = None
                    st.session_state.gen_df.loc[mask,"statut"] = compute_statut(
                        target["date_occurrence"], target["deadline"], None)
                    try:
                        save_parquet(st.session_state.gen_df, "planning_rh/generateur.parquet")
                        st.success("✅ Action remise en cours."); st.rerun()
                    except Exception as e: st.error(f"Erreur R2 : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: PLANNING CALENDRIER
# ══════════════════════════════════════════════════════════════════════════════
elif page_active == "📅 Planning":
    st.markdown("# Planning des actions")
    if gen_df is None or gen_df.empty:
        st.info("Aucune occurrence générée.")
    else:
        gen = gen_df.copy()
        for col in ["date_debut","deadline","date_occurrence","date_fin"]:
            if col in gen.columns:
                gen[col] = pd.to_datetime(gen[col], errors="coerce").dt.date
        today = date.today()
        week_start       = today - timedelta(days=today.weekday())
        week_end         = week_start + timedelta(days=6)
        month_start      = today.replace(day=1)
        month_end        = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
        prev_end         = month_start - timedelta(days=1)
        prev_start       = prev_end.replace(day=1)
        next_start       = date(today.year+(today.month//12),(today.month%12)+1,1)
        next_end         = date(next_start.year, next_start.month, calendar.monthrange(next_start.year, next_start.month)[1])

        for k,v in [("periode","Mois en cours"),("date_debut_custom",month_start),
                    ("date_fin_custom",month_end),("selected_action_id",None),
                    ("selected_date",None),("cal_week_offset",0)]:
            if k not in st.session_state: st.session_state[k]=v

        periodes = ["Semaine en cours","Mois en cours","Mois précédent","Mois suivant","Période spécifique"]
        pc = st.columns(5)
        for i,p in enumerate(periodes):
            with pc[i]:
                if st.button(p, use_container_width=True, type="primary" if st.session_state.periode==p else "secondary", key=f"btn_p_{i}"):
                    st.session_state.periode=p; st.session_state.selected_action_id=None; st.session_state.selected_date=None; st.session_state.cal_week_offset=0; st.rerun()

        if st.session_state.periode=="Période spécifique":
            c1,c2=st.columns(2)
            with c1: st.session_state.date_debut_custom=st.date_input("Du",value=st.session_state.date_debut_custom)
            with c2: st.session_state.date_fin_custom=st.date_input("Au",value=st.session_state.date_fin_custom)
            cal_start,cal_end=st.session_state.date_debut_custom,st.session_state.date_fin_custom
        elif st.session_state.periode=="Semaine en cours": cal_start,cal_end=week_start,week_end
        elif st.session_state.periode=="Mois précédent":  cal_start,cal_end=prev_start,prev_end
        elif st.session_state.periode=="Mois suivant":    cal_start,cal_end=next_start,next_end
        else:                                              cal_start,cal_end=month_start,month_end

        st.markdown("<div style='margin-top:10px'></div>", unsafe_allow_html=True)
        f1,f2,f3,f4=st.columns(4)
        with f1: f_statut=st.multiselect("Statut",STATUTS,default=["En retard","En cours","À venir"])
        with f2: f_type=st.multiselect("Type",gen["type"].dropna().unique().tolist(),default=gen["type"].dropna().unique().tolist())
        with f3: f_resp=st.multiselect("Responsable",gen["responsable"].dropna().unique().tolist(),default=gen["responsable"].dropna().unique().tolist())
        with f4: f_prio=st.multiselect("Priorité",PRIORITES,default=PRIORITES)

        filtered=gen[gen["statut"].isin(f_statut)&gen["type"].isin(f_type)&gen["responsable"].isin(f_resp)&gen["date_occurrence"].apply(lambda d:cal_start<=d<=cal_end if pd.notna(d) else False)].copy()
        if not actions_df.empty:
            pl=actions_df.set_index("id")["priorite"].to_dict()
            filtered["priorite"]=filtered["id_action"].map(pl)
            filtered=filtered[filtered["priorite"].isin(f_prio)]

        all_dates=[]
        d=cal_start
        while d<=cal_end: all_dates.append(d); d+=timedelta(days=1)
        total_days=len(all_dates); use_nav=total_days>7
        if use_nav:
            max_offset=total_days-7
            st.session_state.cal_week_offset=max(0,min(st.session_state.cal_week_offset,max_offset))
            visible_dates=all_dates[st.session_state.cal_week_offset:st.session_state.cal_week_offset+7]
            can_prev=st.session_state.cal_week_offset>0; can_next=st.session_state.cal_week_offset<max_offset
        else:
            visible_dates=all_dates; can_prev=can_next=False

        by_date={d:filtered[filtered["date_occurrence"]==d] for d in all_dates}
        SC={"À venir":{"bg":"#e8f4fd","text":"#0c447c","dot":"#378add"},"En cours":{"bg":"#eaf3de","text":"#3b6d11","dot":"#639922"},"En retard":{"bg":"#fcebeb","text":"#a32d2d","dot":"#e24b4a"},"Fait":{"bg":"#f1efe8","text":"#5f5e5a","dot":"#888780"}}
        JC=["Lun","Mar","Mer","Jeu","Ven","Sam","Dim"]

        pl=st.session_state.cal_week_offset/max(total_days-7,1)*100 if use_nav and total_days>7 else 0
        st.markdown(f'<p style="font-size:11px;color:#8890a8;text-align:center;font-weight:600;letter-spacing:.06em;text-transform:uppercase">{visible_dates[0].strftime("%d/%m")} – {visible_dates[-1].strftime("%d/%m/%Y")}</p>', unsafe_allow_html=True)

        arrow_l=0.4 if (use_nav and can_prev) else 0.0
        arrow_r=0.4 if (use_nav and can_next) else 0.0
        wt=([arrow_l] if arrow_l else [])+[1]*len(visible_dates)+([arrow_r] if arrow_r else [])
        all_cols=st.columns(wt); co=0
        if arrow_l:
            with all_cols[0]:
                st.markdown("<div style='height:8px'></div>",unsafe_allow_html=True)
                if st.button("◀",key="nav_prev",use_container_width=True):
                    st.session_state.cal_week_offset=max(0,st.session_state.cal_week_offset-7); st.rerun()
            co=1
        for i,d in enumerate(visible_dates):
            rows_day=by_date[d]; is_today=d==today
            with all_cols[co+i]:
                hbg="#0f1117" if is_today else "#f5f6fa"; hfg="#e0e4f0" if is_today else "#1a1d2e"; hbd="#378add" if is_today else "#e8eaf0"
                n=len(rows_day); cnt=f" ({n})" if n>0 else ""
                st.markdown(f'<div style="background:{hbg};color:{hfg};text-align:center;border-bottom:2px solid {hbd};border-radius:8px 8px 0 0;padding:6px 4px 4px;font-size:11px;font-weight:600;margin-bottom:2px">{JC[d.weekday()]}<br><span style="font-size:18px;font-weight:700">{d.day}</span><span style="font-size:10px;opacity:.6">/{d.month:02d}</span><br><span style="font-size:10px;opacity:.7">{cnt}</span></div>',unsafe_allow_html=True)
                if st.button("📅" if st.session_state.selected_date==d else "·",key=f"dsel_{d}",use_container_width=True,type="primary" if st.session_state.selected_date==d else "secondary"):
                    st.session_state.selected_date=None if st.session_state.selected_date==d else d; st.session_state.selected_action_id=None; st.rerun()
                if rows_day.empty:
                    st.markdown('<div style="text-align:center;color:#ccc;font-size:11px;padding:6px 0">—</div>',unsafe_allow_html=True)
                else:
                    for _,r in rows_day.iterrows():
                        sc=SC.get(r["statut"],SC["À venir"]); is_sel=st.session_state.selected_action_id==(d,r["id_action"])
                        st.markdown(f'<div style="background:{sc["bg"]};color:{sc["text"]};border-left:3px solid {sc["dot"]};border-radius:0 6px 6px 0;padding:3px 6px;margin:2px 0;font-size:11px;font-weight:600;{"outline:2px solid "+sc["dot"]+";" if is_sel else ""}">#{r["id_action"]}</div>',unsafe_allow_html=True)
                        nom=r["nom_action"]; lbl="#"+str(r["id_action"])+" "+(nom[:10]+"…" if len(nom)>10 else nom)
                        if st.button(lbl,key=f"asel_{d}_{r['id_action']}",use_container_width=True):
                            st.session_state.selected_action_id=None if st.session_state.selected_action_id==(d,r["id_action"]) else (d,r["id_action"]); st.session_state.selected_date=None; st.rerun()
        if arrow_r:
            with all_cols[-1]:
                st.markdown("<div style='height:8px'></div>",unsafe_allow_html=True)
                if st.button("▶",key="nav_next",use_container_width=True):
                    st.session_state.cal_week_offset=min(max_offset,st.session_state.cal_week_offset+7); st.rerun()

        st.markdown('<div style="display:flex;gap:16px;margin-top:8px;flex-wrap:wrap;align-items:center"><span style="font-size:11px;color:#8890a8;font-weight:600">Légende :</span><span style="font-size:11px;background:#e8f4fd;color:#0c447c;border-left:3px solid #378add;padding:2px 8px;border-radius:0 4px 4px 0">À venir</span><span style="font-size:11px;background:#eaf3de;color:#3b6d11;border-left:3px solid #639922;padding:2px 8px;border-radius:0 4px 4px 0">En cours</span><span style="font-size:11px;background:#fcebeb;color:#a32d2d;border-left:3px solid #e24b4a;padding:2px 8px;border-radius:0 4px 4px 0">En retard</span><span style="font-size:11px;background:#f1efe8;color:#5f5e5a;border-left:3px solid #888780;padding:2px 8px;border-radius:0 4px 4px 0">Fait</span></div>',unsafe_allow_html=True)
        st.markdown("---")

        if st.session_state.selected_action_id:
            sd,si=st.session_state.selected_action_id
            detail=filtered[(filtered["id_action"]==si)&(filtered["date_occurrence"]==sd)]
            if not detail.empty:
                r=detail.iloc[0]; sc=SC.get(r["statut"],SC["À venir"])
                ar=actions_df[actions_df["id"]==si].iloc[0] if not actions_df.empty and si in actions_df["id"].values else None
                prio=ar["priorite"] if ar is not None else "—"; freq=ar["frequence"] if ar is not None else "—"
                st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px"><div><div style="font-size:11px;color:var(--color-text-secondary);text-transform:uppercase;font-weight:600;margin-bottom:4px">Détail action</div><div style="font-size:17px;font-weight:600">#{si} — {r["nom_action"]}</div></div><span style="background:{sc["bg"]};color:{sc["text"]};border-left:3px solid {sc["dot"]};padding:5px 14px;border-radius:0 20px 20px 0;font-size:12px;font-weight:600">{r["statut"]}</span></div><div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;font-size:13px"><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Type</div><div>{r["type"]}</div></div><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Responsable</div><div>{r["responsable"]}</div></div><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Priorité</div><div>{prio}</div></div><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Fréquence</div><div>{freq}</div></div><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Occurrence</div><div>{r["date_occurrence"].strftime("%d/%m/%Y")}</div></div><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Date fin</div><div>{r["date_fin"].strftime("%d/%m/%Y") if pd.notna(r["date_fin"]) else "—"}</div></div><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Deadline</div><div>{r["deadline"].strftime("%d/%m/%Y") if pd.notna(r["deadline"]) else "—"}</div></div><div><div style="color:#8890a8;font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Ressource</div><div>{r.get("nom_ressource") or "—"}</div></div></div></div>',unsafe_allow_html=True)
                if r["statut"]!="Fait":
                    if st.button("✅ Marquer comme Fait",type="primary"):
                        mask=((st.session_state.gen_df["id_action"]==si)&(pd.to_datetime(st.session_state.gen_df["date_occurrence"]).dt.date==sd))
                        st.session_state.gen_df.loc[mask,"date_traitement"]=datetime.now(); st.session_state.gen_df.loc[mask,"statut"]="Fait"
                        try: save_parquet(st.session_state.gen_df,"planning_rh/generateur.parquet"); st.success("✅ Fait."); st.session_state.selected_action_id=None; st.rerun()
                        except Exception as e: st.error(f"Erreur R2 : {e}")
        elif st.session_state.selected_date:
            sdate=st.session_state.selected_date; da=filtered[filtered["date_occurrence"]==sdate]
            st.markdown(f"### Actions du {sdate.strftime('%A %d %B %Y')}")
            if da.empty: st.info("Aucune action ce jour.")
            else:
                for _,r in da.iterrows():
                    sc=SC.get(r["statut"],SC["À venir"]); ar=actions_df[actions_df["id"]==r["id_action"]] if not actions_df.empty else pd.DataFrame()
                    prio=ar.iloc[0]["priorite"] if not ar.empty else "—"; dl=r["deadline"].strftime("%d/%m/%Y") if pd.notna(r["deadline"]) else "—"
                    st.markdown(f'<div class="card" style="padding:14px 18px;margin-bottom:8px"><div style="display:flex;justify-content:space-between;align-items:center"><div style="font-size:14px;font-weight:600">#{r["id_action"]} — {r["nom_action"]}</div><span style="background:{sc["bg"]};color:{sc["text"]};border-left:3px solid {sc["dot"]};padding:3px 12px;border-radius:0 20px 20px 0;font-size:11px;font-weight:600">{r["statut"]}</span></div><div style="display:flex;gap:24px;margin-top:8px;font-size:12px;color:#8890a8"><span>Type : <b>{r["type"]}</b></span><span>Resp. : <b>{r["responsable"]}</b></span><span>Priorité : <b>{prio}</b></span><span>Deadline : <b>{dl}</b></span></div></div>',unsafe_allow_html=True)
        else:
            st.markdown('<div style="text-align:center;color:#8890a8;padding:20px;font-size:13px">Cliquez sur une date ou un badge coloré pour voir les détails.</div>',unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: PARAMÉTRAGE TYPES DE CONTRATS
# ══════════════════════════════════════════════════════════════════════════════
elif page_active == "⚙️ Paramétrage types de contrats":
    st.markdown("# ⚙️ Paramétrage des types de contrats")
    st.caption("Définissez les actions proposées par défaut pour chaque type de contrat.")

    tc_sel = st.selectbox("Type de contrat à configurer", TYPES_CONTRAT)
    current = st.session_state.actions_contrat_custom.get(tc_sel, [])

    st.markdown(f'<div class="section-title">Actions par défaut — {tc_sel}</div>', unsafe_allow_html=True)

    # En-têtes
    h=st.columns([2.5,1.8,0.8,1.8,0.8,0.5,0.5])
    for col,lbl in zip(h,["Nom","Type","Durée","Règle","Jour","Edit","Suppr"]):
        col.markdown(f'<div style="font-size:11px;font-weight:600;color:#8890a8;text-transform:uppercase;padding-bottom:4px;border-bottom:1px solid #e8eaf0">{lbl}</div>',unsafe_allow_html=True)

    edit_param_idx = st.session_state.get("edit_param_idx", None)

    for idx, act in enumerate(current):
        c0,c1,c2,c3,c4,c5,c6=st.columns([2.5,1.8,0.8,1.8,0.8,0.5,0.5])
        with c0: st.markdown(f'<div style="padding:6px 0;font-size:13px">{act["nom"]}</div>',unsafe_allow_html=True)
        with c1: st.markdown(f'<div style="padding:6px 0;font-size:12px;color:#8890a8">{act["type"]}</div>',unsafe_allow_html=True)
        with c2: st.markdown(f'<div style="padding:6px 0;font-size:13px">{act["duree"]}j</div>',unsafe_allow_html=True)
        with c3: st.markdown(f'<div style="padding:6px 0;font-size:12px">{act["regle"]}</div>',unsafe_allow_html=True)
        with c4: st.markdown(f'<div style="padding:6px 0;font-size:13px">{act["jour"]}</div>',unsafe_allow_html=True)
        with c5:
            if st.button("✏️",key=f"pedit_{tc_sel}_{idx}",help="Modifier"):
                st.session_state["edit_param_idx"]=idx; st.rerun()
        with c6:
            if st.button("🗑️",key=f"pdel_{tc_sel}_{idx}",help="Supprimer"):
                st.session_state.actions_contrat_custom[tc_sel].pop(idx)
                save_custom_actions(); st.rerun()

    # Formulaire édition
    if edit_param_idx is not None and edit_param_idx < len(current):
        act_e=current[edit_param_idx]
        st.markdown(f'<div class="card" style="margin-top:8px;border-left:3px solid #378add"><b>Modifier l\'action #{edit_param_idx+1}</b></div>',unsafe_allow_html=True)
        with st.form("param_edit_form"):
            ec1,ec2,ec3,ec4,ec5=st.columns([3,2,1,2,1])
            with ec1: pn=st.text_input("Nom",value=act_e["nom"])
            with ec2: pt=st.selectbox("Type",TYPES,index=TYPES.index(act_e["type"]) if act_e["type"] in TYPES else 0)
            with ec3: pd_=st.number_input("Durée",min_value=1,value=int(act_e["duree"]))
            with ec4: pr=st.selectbox("Règle",REGLES_DL,index=REGLES_DL.index(act_e["regle"]) if act_e["regle"] in REGLES_DL else 0)
            with ec5: pj=st.number_input("Jour",min_value=1,max_value=31,value=int(act_e["jour"]))
            s1,s2=st.columns(2)
            with s1:
                if st.form_submit_button("💾 Enregistrer",type="primary",use_container_width=True):
                    st.session_state.actions_contrat_custom[tc_sel][edit_param_idx]={"nom":pn,"type":pt,"duree":pd_,"regle":pr,"jour":pj}
                    st.session_state["edit_param_idx"]=None; save_custom_actions(); st.rerun()
            with s2:
                if st.form_submit_button("✖ Annuler",use_container_width=True):
                    st.session_state["edit_param_idx"]=None; st.rerun()

    # Ajouter une action
    st.markdown("---")
    st.markdown('<div class="section-title">Ajouter une action</div>',unsafe_allow_html=True)
    with st.form("param_add_form"):
        ac1,ac2,ac3,ac4,ac5=st.columns([3,2,1,2,1])
        with ac1: an=st.text_input("Nom de l'action *",placeholder="Ex: Avenant contrat")
        with ac2: at=st.selectbox("Type",TYPES)
        with ac3: ad=st.number_input("Durée",min_value=1,value=1)
        with ac4: ar_=st.selectbox("Règle",REGLES_DL)
        with ac5: aj=st.number_input("Jour",min_value=1,max_value=31,value=1)
        if st.form_submit_button("➕ Ajouter",type="primary",use_container_width=True):
            if not an: st.error("Le nom est obligatoire.")
            else:
                if tc_sel not in st.session_state.actions_contrat_custom:
                    st.session_state.actions_contrat_custom[tc_sel]=[]
                st.session_state.actions_contrat_custom[tc_sel].append({"nom":an,"type":at,"duree":ad,"regle":ar_,"jour":aj})
                save_custom_actions(); st.success(f"Action ajoutée à {tc_sel}."); st.rerun()

    st.markdown("---")
    if st.button("🔄 Réinitialiser ce type aux valeurs par défaut", type="secondary"):
        st.session_state.actions_contrat_custom[tc_sel]=copy.deepcopy(ACTIONS_CONTRAT_DEFAULT.get(tc_sel,[]))
        save_custom_actions(); st.success(f"{tc_sel} réinitialisé."); st.rerun()
