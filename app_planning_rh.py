import streamlit as st
import pandas as pd
import boto3
import io
from datetime import date, datetime, timedelta
import calendar

# ─── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Planning RH",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── STYLES ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
[data-testid="stSidebar"] { background: #0f1117; border-right: 1px solid #1e2130; }
[data-testid="stSidebar"] * { color: #e0e4f0 !important; }
.stApp { background: #f5f6fa; }
.card {
    background: #ffffff; border-radius: 12px; padding: 24px;
    margin-bottom: 16px; border: 1px solid #e8eaf0;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}
.kpi-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
.kpi {
    flex: 1; min-width: 120px; background: #fff; border-radius: 10px;
    padding: 16px 20px; border: 1px solid #e8eaf0;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}
.kpi-label { font-size: 11px; font-weight: 600; letter-spacing: .08em; text-transform: uppercase; color: #8890a8; margin-bottom: 6px; }
.kpi-value { font-size: 28px; font-weight: 600; font-family: 'DM Mono', monospace; color: #1a1d2e; }
.badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 12px; font-weight: 500; font-family: 'DM Mono', monospace;
}
.badge-avenir  { background: #e8f4fd; color: #1a6fa8; }
.badge-encours { background: #e6f9f0; color: #177049; }
.badge-retard  { background: #fde8e8; color: #b81c1c; }
.badge-fait    { background: #f0f0f0; color: #555; }
.prio-haute    { color: #e53935; }
.prio-moyenne  { color: #f59e0b; }
.prio-basse    { color: #6b7280; }
.prio-critique { color: #7c3aed; }
.section-title {
    font-size: 13px; font-weight: 600; letter-spacing: .06em;
    text-transform: uppercase; color: #8890a8; margin-bottom: 14px;
    padding-bottom: 8px; border-bottom: 1px solid #e8eaf0;
}
.stTextInput > div > div > input,
.stSelectbox > div > div,
.stNumberInput > div > div > input,
.stDateInput > div > div > input { border-radius: 8px !important; border-color: #dde0eb !important; }
.stButton > button { border-radius: 8px; font-weight: 600; font-family: 'DM Sans', sans-serif; }
.planning-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.planning-table th {
    background: #0f1117; color: #e0e4f0; padding: 10px 14px;
    text-align: left; font-weight: 500; font-size: 12px;
    letter-spacing: .04em; white-space: nowrap;
}
.planning-table td { padding: 10px 14px; border-bottom: 1px solid #f0f1f5; vertical-align: middle; }
.planning-table tr:hover td { background: #f8f9fc; }
</style>
""", unsafe_allow_html=True)

# ─── PARAMS ────────────────────────────────────────────────────────────────────
TYPES        = ["Paie", "Fiscal", "Administratif RH"]
FREQUENCES   = ["Mensuelle", "Annuelle", "Ponctuelle"]
STATUTS      = ["À venir", "En cours", "Fait", "En retard"]
REGLES_DL    = ["Fin de mois", "M+1", "Date fixe", "J+X"]
RESPONSABLES = ["RH", "Compta", "Direction"]
PRIORITES    = ["Basse", "Moyenne", "Haute", "Critique"]

ACTIONS_COLS = [
    "id", "nom_action", "type", "frequence", "date_debut",
    "duree", "regle_deadline", "jour_deadline", "mois_specifique",
    "responsable", "priorite", "actif", "nom_ressource", "id_ressource"
]
GENERATEUR_COLS = [
    "id_action", "nom_action", "date_occurrence", "date_debut",
    "date_fin", "deadline", "statut", "responsable", "type",
    "nom_ressource", "id_ressource", "date_traitement", "id_contrat"
]
CONTRATS_COLS = [
    "id_contrat", "id_ressource", "nom_ressource", "type_contrat",
    "date_debut", "date_fin", "date_fin_essai", "poste", "salaire",
    "temps_travail", "lieu_travail", "statut_contrat", "notes"
]
TYPES_CONTRAT   = ["CDI", "CDD", "Intérim", "Alternance", "Stage", "Freelance", "Autre"]
TEMPS_TRAVAIL   = ["Temps plein", "Temps partiel 80%", "Temps partiel 50%", "Autre"]
STATUTS_CONTRAT = ["Actif", "À venir", "Terminé", "Suspendu"]

ACTIONS_CONTRAT = {
    "CDI": [
        {"nom": "Signature contrat",         "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Remise équipement",          "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Visite médicale embauche",   "type": "Administratif RH", "duree": 1, "regle": "M+1",        "jour": 30},
        {"nom": "Déclaration DPAE",           "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Inscription mutuelle",       "type": "Administratif RH", "duree": 3, "regle": "M+1",        "jour": 15},
        {"nom": "Fin période d'essai",        "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Entretien période d'essai",  "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Paie du mois",               "type": "Paie",             "duree": 3, "regle": "Fin de mois","jour": 30},
        {"nom": "Solde de tout compte",       "type": "Paie",             "duree": 2, "regle": "M+1",        "jour": 5},
        {"nom": "Attestation employeur",      "type": "Administratif RH", "duree": 1, "regle": "M+1",        "jour": 5},
    ],
    "CDD": [
        {"nom": "Signature contrat",         "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Déclaration DPAE",          "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Visite médicale embauche",  "type": "Administratif RH", "duree": 1, "regle": "M+1",        "jour": 30},
        {"nom": "Inscription mutuelle",      "type": "Administratif RH", "duree": 3, "regle": "M+1",        "jour": 15},
        {"nom": "Fin période d'essai",       "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Paie du mois",              "type": "Paie",             "duree": 3, "regle": "Fin de mois","jour": 30},
        {"nom": "Renouvellement CDD",        "type": "Administratif RH", "duree": 2, "regle": "Date fixe",  "jour": 1},
        {"nom": "Solde de tout compte",      "type": "Paie",             "duree": 2, "regle": "M+1",        "jour": 5},
        {"nom": "Attestation employeur",     "type": "Administratif RH", "duree": 1, "regle": "M+1",        "jour": 5},
        {"nom": "Certificat de travail",     "type": "Administratif RH", "duree": 1, "regle": "M+1",        "jour": 5},
    ],
    "Alternance": [
        {"nom": "Signature contrat",         "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Déclaration DPAE",          "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Enregistrement OPCO",       "type": "Administratif RH", "duree": 5, "regle": "M+1",        "jour": 15},
        {"nom": "Visite médicale",           "type": "Administratif RH", "duree": 1, "regle": "M+1",        "jour": 30},
        {"nom": "Paie du mois",              "type": "Paie",             "duree": 3, "regle": "Fin de mois","jour": 30},
        {"nom": "Bilan mi-parcours",         "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Fin de contrat",            "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
    ],
    "Stage": [
        {"nom": "Convention de stage",            "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Déclaration accueil stagiaire",  "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Gratification mensuelle",        "type": "Paie",             "duree": 2, "regle": "Fin de mois","jour": 30},
        {"nom": "Bilan mi-stage",                 "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Attestation de stage",           "type": "Administratif RH", "duree": 1, "regle": "M+1",        "jour": 5},
    ],
    "Intérim": [
        {"nom": "Contrat de mission",        "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Déclaration DPAE",          "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Paie du mois",              "type": "Paie",             "duree": 3, "regle": "Fin de mois","jour": 30},
        {"nom": "Fin de mission",            "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
    ],
    "Freelance": [
        {"nom": "Signature contrat prestation","type": "Administratif RH","duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Facture mensuelle",           "type": "Fiscal",           "duree": 2, "regle": "Fin de mois","jour": 30},
        {"nom": "Fin de mission",              "type": "Administratif RH","duree": 1, "regle": "Date fixe",  "jour": 1},
    ],
    "Autre": [
        {"nom": "Signature contrat",         "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Déclaration DPAE",          "type": "Administratif RH", "duree": 1, "regle": "Date fixe",  "jour": 1},
        {"nom": "Paie du mois",              "type": "Paie",             "duree": 3, "regle": "Fin de mois","jour": 30},
    ],
}

# ─── R2 CLIENT (boto3) ─────────────────────────────────────────────────────────
# Secrets à configurer dans Streamlit Community Cloud > Settings > Secrets :
#   R2_ACCOUNT_ID = "a262c0d96a51c4a4bcc0e68480df9ec5"
#   R2_ACCESS_KEY = "39d6131812a47246e744bfbc6babb039"
#   R2_SECRET_KEY = "8b25d7231a475956488f89b992e4fefbf342889766f3003fc82c969ae1ad89c9"
#   R2_BUCKET     = "apprh"

@st.cache_resource
def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{st.secrets['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=st.secrets["R2_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["R2_SECRET_KEY"],
        region_name="auto",
    )

def load_parquet(key: str, cols: list) -> pd.DataFrame:
    try:
        s3  = get_r2_client()
        obj = s3.get_object(Bucket=st.secrets["R2_BUCKET"], Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        return pd.DataFrame(columns=cols)

def save_parquet(df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3 = get_r2_client()
    s3.put_object(Bucket=st.secrets["R2_BUCKET"], Key=key, Body=buf.getvalue())

# ─── BUSINESS LOGIC ────────────────────────────────────────────────────────────
def compute_deadline(action: dict, occurrence_date: date) -> date:
    regle = action["regle_deadline"]
    jour  = int(action["jour_deadline"] or 1)
    if regle == "Fin de mois":
        last = calendar.monthrange(occurrence_date.year, occurrence_date.month)[1]
        return occurrence_date.replace(day=last)
    elif regle == "M+1":
        next_month = occurrence_date.replace(day=1) + timedelta(days=32)
        next_month = next_month.replace(day=1)
        last = calendar.monthrange(next_month.year, next_month.month)[1]
        return next_month.replace(day=min(jour, last))
    elif regle == "Date fixe":
        mois = int(action.get("mois_specifique") or occurrence_date.month)
        last = calendar.monthrange(occurrence_date.year, mois)[1]
        return occurrence_date.replace(month=mois, day=min(jour, last))
    elif regle == "J+X":
        return occurrence_date + timedelta(days=jour)
    return occurrence_date

def compute_statut(date_debut: date, deadline: date, date_traitement) -> str:
    today = date.today()
    if pd.notna(date_traitement) and date_traitement:
        return "Fait"
    if today > deadline:
        return "En retard"
    if today >= date_debut:
        return "En cours"
    return "À venir"

def generate_occurrences(actions_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, action in actions_df.iterrows():
        if str(action.get("actif", "Oui")).lower() != "oui":
            continue
        freq  = action.get("frequence", "Mensuelle")
        debut = pd.to_datetime(action["date_debut"]).date()
        n_months = 12 if freq == "Mensuelle" else 1
        for i in range(n_months):
            month = debut.month + i
            year  = debut.year + (month - 1) // 12
            month = ((month - 1) % 12) + 1
            try:
                occ_date = debut.replace(year=year, month=month)
            except ValueError:
                occ_date = date(year, month, calendar.monthrange(year, month)[1])
            date_fin = occ_date + timedelta(days=int(action.get("duree", 1)))
            dl       = compute_deadline(action.to_dict(), occ_date)
            statut   = compute_statut(occ_date, dl, action.get("date_traitement"))
            rows.append({
                "id_action": action["id"], "nom_action": action["nom_action"],
                "date_occurrence": occ_date, "date_debut": occ_date,
                "date_fin": date_fin, "deadline": dl, "statut": statut,
                "responsable": action.get("responsable", ""),
                "type": action.get("type", ""),
                "nom_ressource": action.get("nom_ressource", ""),
                "id_ressource": action.get("id_ressource", None),
                "date_traitement": None,
                "id_contrat": None,
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=GENERATEUR_COLS)

# ─── SESSION STATE ─────────────────────────────────────────────────────────────
if "actions_df" not in st.session_state:
    st.session_state.actions_df = None
if "gen_df" not in st.session_state:
    st.session_state.gen_df = None
if "contrats_df" not in st.session_state:
    st.session_state.contrats_df = None

def load_data():
    actions_df  = load_parquet("planning_rh/actions.parquet", ACTIONS_COLS)
    gen_df      = load_parquet("planning_rh/generateur.parquet", GENERATEUR_COLS)
    contrats_df = load_parquet("planning_rh/contrats.parquet", CONTRATS_COLS)
    for df, key in [(actions_df, "planning_rh/actions.parquet"),
                    (gen_df,     "planning_rh/generateur.parquet"),
                    (contrats_df,"planning_rh/contrats.parquet")]:
        if df.empty:
            try: save_parquet(df, key)
            except Exception as e: st.warning(f"⚠️ Impossible de créer {key} : {e}")
    st.session_state.actions_df  = actions_df
    st.session_state.gen_df      = gen_df
    st.session_state.contrats_df = contrats_df

def reload_and_regen():
    gen_df = generate_occurrences(st.session_state.actions_df)
    st.session_state.gen_df = gen_df
    save_parquet(st.session_state.actions_df, "planning_rh/actions.parquet")
    save_parquet(gen_df, "planning_rh/generateur.parquet")

# ─── SIDEBAR NAV ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📋 Planning RH")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        ["🏠 Tableau de bord", "📄 Nouveau contrat", "➕ Nouvelle action", "✏️ Gérer les actions", "📅 Planning"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    if st.button("🔄 Recharger les données", use_container_width=True):
        load_data()
        st.success("Données rechargées.")

if st.session_state.actions_df is None:
    load_data()

actions_df  = st.session_state.actions_df
gen_df      = st.session_state.gen_df
contrats_df = st.session_state.contrats_df

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: TABLEAU DE BORD
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Tableau de bord":
    st.markdown("# Tableau de bord")
    if gen_df.empty:
        st.info("Aucune donnée disponible. Commencez par créer des actions.")
    else:
        gen = gen_df.copy()
        for col in ["date_debut", "deadline", "date_occurrence"]:
            if col in gen.columns:
                gen[col] = pd.to_datetime(gen[col], errors="coerce").dt.date

        today     = date.today()
        total     = len(gen)
        en_retard = (gen["statut"] == "En retard").sum()
        en_cours  = (gen["statut"] == "En cours").sum()
        a_venir   = (gen["statut"] == "À venir").sum()
        fait      = (gen["statut"] == "Fait").sum()

        st.markdown(f"""
        <div class="kpi-row">
            <div class="kpi"><div class="kpi-label">Total occurrences</div><div class="kpi-value">{total}</div></div>
            <div class="kpi"><div class="kpi-label">En retard</div><div class="kpi-value" style="color:#e53935">{en_retard}</div></div>
            <div class="kpi"><div class="kpi-label">En cours</div><div class="kpi-value" style="color:#177049">{en_cours}</div></div>
            <div class="kpi"><div class="kpi-label">À venir</div><div class="kpi-value" style="color:#1a6fa8">{a_venir}</div></div>
            <div class="kpi"><div class="kpi-label">Fait</div><div class="kpi-value" style="color:#8890a8">{fait}</div></div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="section-title">⚠️ Actions urgentes</div>', unsafe_allow_html=True)
        urgentes = gen[
            (gen["statut"].isin(["En retard", "En cours"])) &
            (gen["deadline"] <= today + timedelta(days=7))
        ].sort_values("deadline")

        if urgentes.empty:
            st.success("Aucune action urgente 🎉")
        else:
            badge_map = {
                "À venir":   ("badge-avenir",  "À venir"),
                "En cours":  ("badge-encours", "En cours"),
                "En retard": ("badge-retard",  "En retard"),
                "Fait":      ("badge-fait",    "Fait"),
            }
            rows_html = ""
            for _, r in urgentes.iterrows():
                cls, lbl = badge_map.get(r["statut"], ("badge-avenir", r["statut"]))
                dl = r["deadline"].strftime("%d/%m/%Y") if pd.notna(r["deadline"]) else "—"
                rows_html += f"""
                <tr>
                    <td><b>{r['nom_action']}</b></td><td>{r['type']}</td>
                    <td>{r['responsable']}</td><td>{dl}</td>
                    <td><span class="badge {cls}">{lbl}</span></td>
                </tr>"""
            st.markdown(f"""
            <div class="card" style="padding:0;overflow:hidden">
            <table class="planning-table">
                <thead><tr><th>Action</th><th>Type</th><th>Responsable</th><th>Deadline</th><th>Statut</th></tr></thead>
                <tbody>{rows_html}</tbody>
            </table></div>""", unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-title">Par type</div>', unsafe_allow_html=True)
            st.dataframe(gen.groupby("type")["statut"].value_counts().unstack(fill_value=0), use_container_width=True)
        with col2:
            st.markdown('<div class="section-title">Par responsable</div>', unsafe_allow_html=True)
            st.dataframe(gen.groupby("responsable")["statut"].value_counts().unstack(fill_value=0), use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: NOUVEAU CONTRAT
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📄 Nouveau contrat":
    st.markdown("# Nouveau contrat")

    if contrats_df is None:
        contrats_df = pd.DataFrame(columns=CONTRATS_COLS)

    next_contrat_id = 1 if contrats_df.empty or "id_contrat" not in contrats_df.columns else int(contrats_df["id_contrat"].max()) + 1

    st.markdown(
        f'<div class="card"><span style="font-size:12px;color:#8890a8;font-family:\'DM Mono\',monospace">'
        f'ID contrat auto-assigné : <b>#{next_contrat_id}</b></span></div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="section-title">Étape 1 — Informations du contrat</div>', unsafe_allow_html=True)

    with st.form("form_contrat", clear_on_submit=False):
        st.markdown('<div class="section-title">Ressource</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1: id_ressource  = st.text_input("ID Ressource *", placeholder="Ex: EMP-001")
        with c2: nom_ressource = st.text_input("Nom complet *",  placeholder="Ex: Jean Dupont")
        with c3: poste         = st.text_input("Poste / Fonction", placeholder="Ex: Développeur")

        st.markdown('<div class="section-title">Contrat</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1: type_contrat   = st.selectbox("Type de contrat *", TYPES_CONTRAT)
        with c2: temps_travail  = st.selectbox("Temps de travail", TEMPS_TRAVAIL)
        with c3: statut_contrat = st.selectbox("Statut", STATUTS_CONTRAT)

        st.markdown('<div class="section-title">Dates</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1: date_debut_c   = st.date_input("Date de début *", value=date.today())
        with c2: date_fin_essai = st.date_input("Fin période d'essai", value=date.today() + timedelta(days=90))
        with c3: has_date_fin   = st.checkbox("Date de fin prévue ?", value=False)

        date_fin_c = None
        if has_date_fin:
            date_fin_c = st.date_input("Date de fin de contrat", value=date.today() + timedelta(days=365))

        st.markdown('<div class="section-title">Autres informations</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1: salaire      = st.number_input("Salaire brut mensuel (€)", min_value=0, value=0, step=100)
        with c2: lieu_travail = st.text_input("Lieu de travail", placeholder="Ex: Paris / Remote")
        with c3: notes        = st.text_area("Notes", placeholder="Informations complémentaires...", height=80)

        valider_etape1 = st.form_submit_button("Suivant — Choisir les actions ▶", use_container_width=True, type="primary")

    if valider_etape1:
        if not id_ressource or not nom_ressource:
            st.error("L'ID et le nom de la ressource sont obligatoires.")
        else:
            st.session_state["contrat_draft"] = {
                "id_contrat": next_contrat_id, "id_ressource": id_ressource,
                "nom_ressource": nom_ressource, "type_contrat": type_contrat,
                "date_debut": date_debut_c, "date_fin": date_fin_c,
                "date_fin_essai": date_fin_essai, "poste": poste,
                "salaire": salaire, "temps_travail": temps_travail,
                "lieu_travail": lieu_travail, "statut_contrat": statut_contrat,
                "notes": notes,
            }

    # ── Étape 2 : Sélection des actions ──────────────────────────────────────
    if "contrat_draft" in st.session_state:
        draft = st.session_state["contrat_draft"]
        tc    = draft["type_contrat"]
        actions_proposees = ACTIONS_CONTRAT.get(tc, [])

        st.markdown("---")
        st.markdown(
            f'<div class="section-title">Étape 2 — Actions proposées pour un contrat {tc} '
            f'· {draft["nom_ressource"]}</div>',
            unsafe_allow_html=True,
        )
        st.caption("Cochez les actions à générer automatiquement. Vous pourrez les modifier ensuite dans Gérer les actions.")

        # En-tête tableau
        h0, h1, h2, h3, h4, h5 = st.columns([0.3, 3, 2, 1, 2, 1])
        for col, lbl in zip([h0,h1,h2,h3,h4,h5], ["✓","Nom de l'action","Type","Durée","Règle deadline","Jour"]):
            col.markdown(f'<div style="font-size:11px;font-weight:600;color:#8890a8;text-transform:uppercase;padding-bottom:4px;border-bottom:1px solid #e8eaf0">{lbl}</div>', unsafe_allow_html=True)

        checks = {}
        for idx, act in enumerate(actions_proposees):
            c0, c1, c2, c3, c4, c5 = st.columns([0.3, 3, 2, 1, 2, 1])
            with c0: checks[idx] = st.checkbox("", value=True, key=f"chk_act_{idx}", label_visibility="collapsed")
            with c1:
                bg = "#e6f9f0" if checks.get(idx, True) else "#f5f6fa"
                st.markdown(f'<div style="padding:6px 0;font-size:13px;font-weight:500">{act["nom"]}</div>', unsafe_allow_html=True)
            with c2: st.markdown(f'<div style="padding:6px 0;font-size:12px;color:#8890a8">{act["type"]}</div>', unsafe_allow_html=True)
            with c3: st.markdown(f'<div style="padding:6px 0;font-size:13px">{act["duree"]}j</div>', unsafe_allow_html=True)
            with c4: st.markdown(f'<div style="padding:6px 0;font-size:12px">{act["regle"]}</div>', unsafe_allow_html=True)
            with c5: st.markdown(f'<div style="padding:6px 0;font-size:13px">{act["jour"]}</div>', unsafe_allow_html=True)

        selected_actions = [actions_proposees[i] for i, checked in checks.items() if checked]
        st.markdown(f'<div style="margin-top:8px;font-size:13px;color:#8890a8"><b>{len(selected_actions)}</b> action(s) sélectionnée(s)</div>', unsafe_allow_html=True)

        st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ Créer le contrat et les actions", type="primary", use_container_width=True):
                if not selected_actions:
                    st.warning("Sélectionnez au moins une action.")
                else:
                    new_contrat = {
                        "id_contrat":    draft["id_contrat"],
                        "id_ressource":  draft["id_ressource"],
                        "nom_ressource": draft["nom_ressource"],
                        "type_contrat":  draft["type_contrat"],
                        "date_debut":    pd.Timestamp(draft["date_debut"]),
                        "date_fin":      pd.Timestamp(draft["date_fin"]) if draft["date_fin"] else None,
                        "date_fin_essai":pd.Timestamp(draft["date_fin_essai"]),
                        "poste":         draft["poste"],
                        "salaire":       draft["salaire"],
                        "temps_travail": draft["temps_travail"],
                        "lieu_travail":  draft["lieu_travail"],
                        "statut_contrat":draft["statut_contrat"],
                        "notes":         draft["notes"],
                    }
                    st.session_state.contrats_df = pd.concat(
                        [st.session_state.contrats_df, pd.DataFrame([new_contrat])], ignore_index=True
                    )
                    next_action_id = (
                        1 if actions_df.empty or "id" not in actions_df.columns
                        else int(actions_df["id"].max()) + 1
                    )
                    new_actions = []
                    for i, act in enumerate(selected_actions):
                        new_actions.append({
                            "id": next_action_id + i,
                            "nom_action":     act["nom"] + f" — {draft['nom_ressource']}",
                            "type":           act["type"],
                            "frequence":      "Ponctuelle",
                            "date_debut":     pd.Timestamp(draft["date_debut"]),
                            "duree":          act["duree"],
                            "regle_deadline": act["regle"],
                            "jour_deadline":  act["jour"],
                            "mois_specifique":None,
                            "responsable":    "RH",
                            "priorite":       "Haute",
                            "actif":          "Oui",
                            "nom_ressource":  draft["nom_ressource"],
                            "id_ressource":   draft["id_ressource"],
                        })
                    st.session_state.actions_df = pd.concat(
                        [actions_df, pd.DataFrame(new_actions)], ignore_index=True
                    )
                    try:
                        gen_new = generate_occurrences(st.session_state.actions_df)
                        new_ids = [a["id"] for a in new_actions]
                        gen_new.loc[gen_new["id_action"].isin(new_ids), "id_contrat"] = draft["id_contrat"]
                        st.session_state.gen_df = gen_new
                        save_parquet(st.session_state.actions_df,  "planning_rh/actions.parquet")
                        save_parquet(st.session_state.gen_df,       "planning_rh/generateur.parquet")
                        save_parquet(st.session_state.contrats_df,  "planning_rh/contrats.parquet")
                        del st.session_state["contrat_draft"]
                        st.success(f"✅ Contrat #{draft['id_contrat']} créé avec {len(selected_actions)} action(s) !")
                        st.balloons()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erreur sauvegarde R2 : {e}")
        with c2:
            if st.button("✖ Annuler", use_container_width=True):
                del st.session_state["contrat_draft"]
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: NOUVELLE ACTION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "➕ Nouvelle action":
    st.markdown("# Nouvelle action")
    next_id = 1 if (actions_df.empty or "id" not in actions_df.columns) else int(actions_df["id"].max()) + 1
    st.markdown(f'<div class="card"><span style="font-size:12px;color:#8890a8;font-family:\'DM Mono\',monospace">ID auto-assigné : <b>#{next_id}</b></span></div>', unsafe_allow_html=True)

    with st.form("form_action", clear_on_submit=True):
        st.markdown('<div class="section-title">Identification</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1: nom_action    = st.text_input("Nom de l'action *", placeholder="Ex: Paie mensuelle")
        with c2: nom_ressource = st.text_input("Nom de la ressource", placeholder="Ex: Employé X")

        st.markdown('<div class="section-title">Classification</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1: type_action = st.selectbox("Type *", TYPES)
        with c2: frequence   = st.selectbox("Fréquence *", FREQUENCES)
        with c3: priorite    = st.selectbox("Priorité *", PRIORITES, index=2)

        st.markdown('<div class="section-title">Dates & Durée</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1: date_debut = st.date_input("Date de début *", value=date.today())
        with c2: duree      = st.number_input("Durée (jours) *", min_value=1, max_value=365, value=3)

        st.markdown('<div class="section-title">Règle de deadline</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1: regle_deadline  = st.selectbox("Règle deadline *", REGLES_DL)
        with c2: jour_deadline   = st.number_input("Jour deadline", min_value=1, max_value=31, value=30)
        with c3: mois_specifique = st.number_input("Mois spécifique (si Date fixe)", min_value=0, max_value=12, value=0)

        st.markdown('<div class="section-title">Responsable & Statut</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1: responsable = st.selectbox("Responsable *", RESPONSABLES)
        with c2: actif       = st.selectbox("Actif", ["Oui", "Non"])

        submitted = st.form_submit_button("✅ Créer l'action", use_container_width=True, type="primary")

    if submitted:
        if not nom_action:
            st.error("Le nom de l'action est obligatoire.")
        else:
            new_row = {
                "id": next_id, "nom_action": nom_action, "type": type_action,
                "frequence": frequence, "date_debut": pd.Timestamp(date_debut),
                "duree": duree, "regle_deadline": regle_deadline,
                "jour_deadline": jour_deadline,
                "mois_specifique": mois_specifique if mois_specifique > 0 else None,
                "responsable": responsable, "priorite": priorite,
                "actif": actif, "nom_ressource": nom_ressource or None,
            }
            st.session_state.actions_df = pd.concat([actions_df, pd.DataFrame([new_row])], ignore_index=True)
            try:
                reload_and_regen()
                st.success(f"✅ Action **#{next_id} — {nom_action}** créée et planning régénéré !")
                st.balloons()
            except Exception as e:
                st.error(f"Erreur lors de la sauvegarde R2 : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: GÉRER LES ACTIONS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "✏️ Gérer les actions":
    st.markdown("# Gérer les actions")
    if actions_df.empty:
        st.info("Aucune action enregistrée.")
    else:
        display_df = actions_df.copy()
        display_df["date_debut"] = pd.to_datetime(display_df["date_debut"]).dt.strftime("%d/%m/%Y")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.markdown("---")
        st.markdown("### Modifier / Supprimer une action")

        action_ids  = actions_df["id"].tolist()
        selected_id = st.selectbox(
            "Sélectionner une action par ID", options=action_ids,
            format_func=lambda i: f"#{i} — {actions_df[actions_df['id']==i]['nom_action'].values[0]}"
        )
        row = actions_df[actions_df["id"] == selected_id].iloc[0]
        tab_edit, tab_del = st.tabs(["✏️ Modifier", "🗑️ Supprimer"])

        with tab_edit:
            with st.form("edit_form"):
                c1, c2 = st.columns(2)
                with c1: nom_action    = st.text_input("Nom de l'action", value=row["nom_action"])
                with c2: nom_ressource = st.text_input("Nom ressource", value=str(row.get("nom_ressource") or ""))
                c1, c2, c3 = st.columns(3)
                with c1: type_action = st.selectbox("Type", TYPES, index=TYPES.index(row["type"]) if row["type"] in TYPES else 0)
                with c2: frequence   = st.selectbox("Fréquence", FREQUENCES, index=FREQUENCES.index(row["frequence"]) if row["frequence"] in FREQUENCES else 0)
                with c3: priorite    = st.selectbox("Priorité", PRIORITES, index=PRIORITES.index(row["priorite"]) if row["priorite"] in PRIORITES else 2)
                c1, c2 = st.columns(2)
                with c1: date_debut = st.date_input("Date début", value=pd.to_datetime(row["date_debut"]).date())
                with c2: duree      = st.number_input("Durée (jours)", min_value=1, value=int(row["duree"]))
                c1, c2, c3 = st.columns(3)
                with c1: regle_dl = st.selectbox("Règle deadline", REGLES_DL, index=REGLES_DL.index(row["regle_deadline"]) if row["regle_deadline"] in REGLES_DL else 0)
                with c2: jour_dl  = st.number_input("Jour deadline", min_value=1, max_value=31, value=int(row["jour_deadline"] or 1))
                with c3: mois_sp  = st.number_input("Mois spécifique", min_value=0, max_value=12, value=int(row.get("mois_specifique") or 0))
                c1, c2 = st.columns(2)
                with c1: responsable = st.selectbox("Responsable", RESPONSABLES, index=RESPONSABLES.index(row["responsable"]) if row["responsable"] in RESPONSABLES else 0)
                with c2: actif       = st.selectbox("Actif", ["Oui", "Non"], index=0 if row["actif"] == "Oui" else 1)

                if st.form_submit_button("💾 Sauvegarder", use_container_width=True, type="primary"):
                    idx = st.session_state.actions_df[st.session_state.actions_df["id"] == selected_id].index[0]
                    for k, v in {
                        "nom_action": nom_action, "type": type_action, "frequence": frequence,
                        "date_debut": pd.Timestamp(date_debut), "duree": duree,
                        "regle_deadline": regle_dl, "jour_deadline": jour_dl,
                        "mois_specifique": mois_sp if mois_sp > 0 else None,
                        "responsable": responsable, "priorite": priorite,
                        "actif": actif, "nom_ressource": nom_ressource or None,
                    }.items():
                        st.session_state.actions_df.at[idx, k] = v
                    try:
                        reload_and_regen()
                        st.success("✅ Action mise à jour et planning régénéré.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erreur R2 : {e}")

        with tab_del:
            st.warning(f"Supprimer définitivement **#{selected_id} — {row['nom_action']}** ?")
            if st.button("🗑️ Confirmer la suppression", type="primary"):
                st.session_state.actions_df = st.session_state.actions_df[
                    st.session_state.actions_df["id"] != selected_id
                ].reset_index(drop=True)
                try:
                    reload_and_regen()
                    st.success("Action supprimée.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erreur R2 : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: PLANNING CALENDRIER
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📅 Planning":
    st.markdown("# Planning des actions")

    if gen_df.empty:
        st.info("Aucune occurrence générée. Créez des actions d'abord.")
    else:
        gen = gen_df.copy()
        for col in ["date_debut", "deadline", "date_occurrence", "date_fin"]:
            if col in gen.columns:
                gen[col] = pd.to_datetime(gen[col], errors="coerce").dt.date

        today = date.today()

        # Calcul des périodes
        week_start        = today - timedelta(days=today.weekday())
        week_end          = week_start + timedelta(days=6)
        month_start       = today.replace(day=1)
        month_end         = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
        prev_month_end    = month_start - timedelta(days=1)
        prev_month_start  = prev_month_end.replace(day=1)
        next_month_start  = date(today.year + (today.month // 12), (today.month % 12) + 1, 1)
        next_month_end    = date(next_month_start.year, next_month_start.month,
                                 calendar.monthrange(next_month_start.year, next_month_start.month)[1])

        # Session state
        for k, v in [
            ("periode", "Mois en cours"),
            ("date_debut_custom", month_start),
            ("date_fin_custom", month_end),
            ("selected_action_id", None),
            ("selected_date", None),
        ]:
            if k not in st.session_state:
                st.session_state[k] = v

        # ── Rangée 1 : Période ──────────────────────────────────────────────
        periodes = ["Semaine en cours", "Mois en cours", "Mois précédent", "Mois suivant", "Période spécifique"]
        p_cols = st.columns(5)
        for i, p in enumerate(periodes):
            with p_cols[i]:
                active = st.session_state.periode == p
                if st.button(p, use_container_width=True, type="primary" if active else "secondary", key=f"btn_periode_{i}"):
                    st.session_state.periode = p
                    st.session_state.selected_action_id = None
                    st.session_state.selected_date = None
                    st.rerun()

        if st.session_state.periode == "Période spécifique":
            c1, c2 = st.columns(2)
            with c1:
                st.session_state.date_debut_custom = st.date_input("Du", value=st.session_state.date_debut_custom)
            with c2:
                st.session_state.date_fin_custom = st.date_input("Au", value=st.session_state.date_fin_custom)
            cal_start, cal_end = st.session_state.date_debut_custom, st.session_state.date_fin_custom
        elif st.session_state.periode == "Semaine en cours":
            cal_start, cal_end = week_start, week_end
        elif st.session_state.periode == "Mois précédent":
            cal_start, cal_end = prev_month_start, prev_month_end
        elif st.session_state.periode == "Mois suivant":
            cal_start, cal_end = next_month_start, next_month_end
        else:
            cal_start, cal_end = month_start, month_end

        # ── Rangée 2 : Filtres ───────────────────────────────────────────────
        st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            f_statut = st.multiselect("Statut", STATUTS, default=["En retard", "En cours", "À venir"])
        with f2:
            f_type = st.multiselect("Type", gen["type"].dropna().unique().tolist(), default=gen["type"].dropna().unique().tolist())
        with f3:
            f_resp = st.multiselect("Responsable", gen["responsable"].dropna().unique().tolist(), default=gen["responsable"].dropna().unique().tolist())
        with f4:
            f_prio = st.multiselect("Priorité", PRIORITES, default=PRIORITES)

        # ── Filtrage ─────────────────────────────────────────────────────────
        filtered = gen[
            gen["statut"].isin(f_statut) &
            gen["type"].isin(f_type) &
            gen["responsable"].isin(f_resp) &
            gen["date_occurrence"].apply(lambda d: cal_start <= d <= cal_end if pd.notna(d) else False)
        ].copy()

        if not actions_df.empty:
            prio_lookup = actions_df.set_index("id")["priorite"].to_dict()
            filtered["priorite"] = filtered["id_action"].map(prio_lookup)
            filtered = filtered[filtered["priorite"].isin(f_prio)]

        # Couleurs statut
        STATUT_COLOR = {
            "À venir":   {"bg": "#e8f4fd", "text": "#0c447c", "dot": "#378add"},
            "En cours":  {"bg": "#eaf3de", "text": "#3b6d11", "dot": "#639922"},
            "En retard": {"bg": "#fcebeb", "text": "#a32d2d", "dot": "#e24b4a"},
            "Fait":      {"bg": "#f1efe8", "text": "#5f5e5a", "dot": "#888780"},
        }
        JOURS_COURT = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]

        # ── Navigation semaine ───────────────────────────────────────────────
        all_dates = []
        d = cal_start
        while d <= cal_end:
            all_dates.append(d)
            d += timedelta(days=1)

        total_days = len(all_dates)
        use_nav    = total_days > 7

        if "cal_week_offset" not in st.session_state:
            st.session_state.cal_week_offset = 0

        # Calcule la fenêtre de 7 jours visible
        if use_nav:
            max_offset = total_days - 7
            st.session_state.cal_week_offset = max(0, min(st.session_state.cal_week_offset, max_offset))
            visible_dates = all_dates[st.session_state.cal_week_offset: st.session_state.cal_week_offset + 7]
            can_prev = st.session_state.cal_week_offset > 0
            can_next = st.session_state.cal_week_offset < max_offset
        else:
            visible_dates = all_dates
            can_prev = can_next = False

        by_date = {d: filtered[filtered["date_occurrence"] == d] for d in all_dates}

        # ── Rendu calendrier via colonnes Streamlit ──────────────────────────
        periode_label = (
            visible_dates[0].strftime("%d/%m") + " – " + visible_dates[-1].strftime("%d/%m/%Y")
        )
        st.markdown(
            '<p style="font-size:11px;color:#8890a8;text-align:center;margin-bottom:6px;'
            'font-weight:600;letter-spacing:.06em;text-transform:uppercase">'
            + periode_label + "</p>",
            unsafe_allow_html=True,
        )

        # Flèches + colonnes jours
        arrow_left  = 0.4 if (use_nav and can_prev) else 0.0
        arrow_right = 0.4 if (use_nav and can_next) else 0.0
        day_weight  = [1] * len(visible_dates)
        col_weights = ([arrow_left] if arrow_left else []) + day_weight + ([arrow_right] if arrow_right else [])
        all_cols    = st.columns(col_weights)
        col_offset  = 0

        # Flèche gauche
        if arrow_left:
            with all_cols[0]:
                st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
                if st.button("◀", key="nav_prev", use_container_width=True):
                    st.session_state.cal_week_offset = max(0, st.session_state.cal_week_offset - 7)
                    st.rerun()
            col_offset = 1

        # Colonnes jours
        for i, d in enumerate(visible_dates):
            rows_day = by_date[d]
            is_today = d == today
            col_idx  = col_offset + i

            with all_cols[col_idx]:
                # ── Header date ──────────────────────────────────────────
                hdr_bg  = "#0f1117" if is_today else "#f5f6fa"
                hdr_fg  = "#e0e4f0" if is_today else "#1a1d2e"
                hdr_bdr = "#378add" if is_today else "#e8eaf0"
                n       = len(rows_day)
                cnt     = f" ({n})" if n > 0 else ""
                st.markdown(
                    f'<div style="background:{hdr_bg};color:{hdr_fg};text-align:center;'
                    f'border-bottom:2px solid {hdr_bdr};border-radius:8px 8px 0 0;'
                    f'padding:6px 4px 4px;font-size:11px;font-weight:600;'
                    f'margin-bottom:2px">'
                    f'{JOURS_COURT[d.weekday()]}<br>'
                    f'<span style="font-size:18px;font-weight:700">{d.day}</span>'
                    f'<span style="font-size:10px;opacity:.6">/{d.month:02d}</span>'
                    f'<br><span style="font-size:10px;opacity:.7">{cnt}</span>'
                    f"</div>",
                    unsafe_allow_html=True,
                )

                # Bouton sélection date (invisible sous le header)
                if st.button(
                    "📅" if st.session_state.selected_date == d else "·",
                    key=f"dsel_{d}",
                    use_container_width=True,
                    type="primary" if st.session_state.selected_date == d else "secondary",
                ):
                    st.session_state.selected_date = (
                        None if st.session_state.selected_date == d else d
                    )
                    st.session_state.selected_action_id = None
                    st.rerun()

                # ── Actions du jour ───────────────────────────────────────
                if rows_day.empty:
                    st.markdown(
                        '<div style="text-align:center;color:#ccc;font-size:11px;padding:6px 0">—</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    for _, r in rows_day.iterrows():
                        sc     = STATUT_COLOR.get(r["statut"], STATUT_COLOR["À venir"])
                        is_sel = st.session_state.selected_action_id == (d, r["id_action"])
                        outline = f"outline:2px solid {sc['dot']};" if is_sel else ""
                        # Badge visuel coloré
                        st.markdown(
                            f'<div style="background:{sc["bg"]};color:{sc["text"]};'
                            f'border-left:3px solid {sc["dot"]};border-radius:0 6px 6px 0;'
                            f'padding:3px 6px;margin:2px 0;font-size:11px;font-weight:600;{outline}">'
                            f'#{r["id_action"]}</div>',
                            unsafe_allow_html=True,
                        )
                        # Bouton cliquable sous le badge
                        nom = r["nom_action"]
                        lbl = ("#" + str(r["id_action"]) + " " + (nom[:10] + "…" if len(nom) > 10 else nom))
                        if st.button(lbl, key=f"asel_{d}_{r['id_action']}", use_container_width=True):
                            if st.session_state.selected_action_id == (d, r["id_action"]):
                                st.session_state.selected_action_id = None
                            else:
                                st.session_state.selected_action_id = (d, r["id_action"])
                                st.session_state.selected_date = None
                            st.rerun()

        # Flèche droite
        if arrow_right:
            with all_cols[-1]:
                st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
                if st.button("▶", key="nav_next", use_container_width=True):
                    st.session_state.cal_week_offset = min(
                        max_offset, st.session_state.cal_week_offset + 7
                    )
                    st.rerun()

        # ── Légende ──────────────────────────────────────────────────────────
        st.markdown("""
        <div style="display:flex;gap:16px;margin-top:8px;flex-wrap:wrap;align-items:center">
            <span style="font-size:11px;color:#8890a8;font-weight:600">Légende :</span>
            <span style="font-size:11px;background:#e8f4fd;color:#0c447c;border-left:3px solid #378add;padding:2px 8px;border-radius:0 4px 4px 0">À venir</span>
            <span style="font-size:11px;background:#eaf3de;color:#3b6d11;border-left:3px solid #639922;padding:2px 8px;border-radius:0 4px 4px 0">En cours</span>
            <span style="font-size:11px;background:#fcebeb;color:#a32d2d;border-left:3px solid #e24b4a;padding:2px 8px;border-radius:0 4px 4px 0">En retard</span>
            <span style="font-size:11px;background:#f1efe8;color:#5f5e5a;border-left:3px solid #888780;padding:2px 8px;border-radius:0 4px 4px 0">Fait</span>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        # ── Détail action ────────────────────────────────────────────────────
        if st.session_state.selected_action_id:
            sel_date, sel_id = st.session_state.selected_action_id
            detail = filtered[(filtered["id_action"] == sel_id) & (filtered["date_occurrence"] == sel_date)]
            if not detail.empty:
                r  = detail.iloc[0]
                sc = STATUT_COLOR.get(r["statut"], STATUT_COLOR["À venir"])
                act_row = actions_df[actions_df["id"] == sel_id].iloc[0] if not actions_df.empty and sel_id in actions_df["id"].values else None
                prio = act_row["priorite"] if act_row is not None else "—"
                freq = act_row["frequence"] if act_row is not None else "—"

                st.markdown(f"""
                <div class="card">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
                        <div>
                            <div style="font-size:11px;color:var(--color-text-secondary);text-transform:uppercase;font-weight:600;letter-spacing:.06em;margin-bottom:4px">Détail action</div>
                            <div style="font-size:17px;font-weight:600">#{sel_id} — {r['nom_action']}</div>
                        </div>
                        <span style="background:{sc['bg']};color:{sc['text']};border-left:3px solid {sc['dot']};
                              padding:5px 14px;border-radius:0 20px 20px 0;font-size:12px;font-weight:600">{r['statut']}</span>
                    </div>
                    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;font-size:13px">
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Type</div><div>{r['type']}</div></div>
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Responsable</div><div>{r['responsable']}</div></div>
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Priorité</div><div>{prio}</div></div>
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Fréquence</div><div>{freq}</div></div>
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Occurrence</div><div>{r['date_occurrence'].strftime('%d/%m/%Y')}</div></div>
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Date fin</div><div>{r['date_fin'].strftime('%d/%m/%Y') if pd.notna(r['date_fin']) else '—'}</div></div>
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Deadline</div><div>{r['deadline'].strftime('%d/%m/%Y') if pd.notna(r['deadline']) else '—'}</div></div>
                        <div><div style="color:var(--color-text-secondary);font-size:11px;text-transform:uppercase;font-weight:600;margin-bottom:3px">Ressource</div><div>{r['nom_ressource'] or '—'}</div></div>
                    </div>
                </div>""", unsafe_allow_html=True)

                if r["statut"] != "Fait":
                    if st.button("✅ Marquer comme Fait", type="primary"):
                        mask = (
                            (st.session_state.gen_df["id_action"] == sel_id) &
                            (pd.to_datetime(st.session_state.gen_df["date_occurrence"]).dt.date == sel_date)
                        )
                        st.session_state.gen_df.loc[mask, "date_traitement"] = datetime.now()
                        st.session_state.gen_df.loc[mask, "statut"] = "Fait"
                        try:
                            save_parquet(st.session_state.gen_df, "planning_rh/generateur.parquet")
                            st.success("✅ Marqué comme Fait.")
                            st.session_state.selected_action_id = None
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erreur R2 : {e}")

        elif st.session_state.selected_date:
            sel_date   = st.session_state.selected_date
            day_actions = filtered[filtered["date_occurrence"] == sel_date]
            st.markdown(f"### Actions du {sel_date.strftime('%A %d %B %Y')}")
            if day_actions.empty:
                st.info("Aucune action ce jour.")
            else:
                for _, r in day_actions.iterrows():
                    sc = STATUT_COLOR.get(r["statut"], STATUT_COLOR["À venir"])
                    act_row = actions_df[actions_df["id"] == r["id_action"]] if not actions_df.empty else pd.DataFrame()
                    prio = act_row.iloc[0]["priorite"] if not act_row.empty else "—"
                    dl   = r["deadline"].strftime("%d/%m/%Y") if pd.notna(r["deadline"]) else "—"
                    st.markdown(f"""
                    <div class="card" style="padding:14px 18px;margin-bottom:8px">
                        <div style="display:flex;justify-content:space-between;align-items:center">
                            <div style="font-size:14px;font-weight:600">#{r['id_action']} — {r['nom_action']}</div>
                            <span style="background:{sc['bg']};color:{sc['text']};border-left:3px solid {sc['dot']};
                                  padding:3px 12px;border-radius:0 20px 20px 0;font-size:11px;font-weight:600">{r['statut']}</span>
                        </div>
                        <div style="display:flex;gap:24px;margin-top:8px;font-size:12px;color:var(--color-text-secondary)">
                            <span>Type : <b>{r['type']}</b></span>
                            <span>Resp. : <b>{r['responsable']}</b></span>
                            <span>Priorité : <b>{prio}</b></span>
                            <span>Deadline : <b>{dl}</b></span>
                        </div>
                    </div>""", unsafe_allow_html=True)

        else:
            st.markdown(
                '<div style="text-align:center;color:var(--color-text-tertiary);padding:20px;font-size:13px">'
                'Cliquez sur une date pour voir toutes ses actions, ou sur un badge coloré pour le détail d\'une action.'
                '</div>',
                unsafe_allow_html=True
            )
