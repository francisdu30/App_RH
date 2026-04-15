import streamlit as st
import pandas as pd
import io
import requests
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
    "responsable", "priorite", "actif", "nom_ressource"
]
GENERATEUR_COLS = [
    "id_action", "nom_action", "date_occurrence", "date_debut",
    "date_fin", "deadline", "statut", "responsable", "type",
    "nom_ressource", "date_traitement"
]



def _headers():
    return {"Authorization": f"Bearer {st.secrets['R2_TOKEN']}"}

def _url(key: str) -> str:
    return (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{st.secrets['R2_ACCOUNT_ID']}/r2/buckets/{st.secrets['R2_BUCKET']}/objects/{key}"
    )

def load_parquet(key: str, cols: list) -> pd.DataFrame:
    try:
        resp = requests.get(_url(key), headers=_headers(), timeout=15)
        if resp.status_code == 200:
            return pd.read_parquet(io.BytesIO(resp.content))
        return pd.DataFrame(columns=cols)
    except Exception:
        return pd.DataFrame(columns=cols)

def save_parquet(df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    resp = requests.put(
        _url(key),
        headers={**_headers(), "Content-Type": "application/octet-stream"},
        data=buf.getvalue(),
        timeout=15
    )
    if resp.status_code not in (200, 201):
        raise Exception(f"Erreur upload R2 : {resp.status_code} — {resp.text}")

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
                "date_traitement": None,
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=GENERATEUR_COLS)

# ─── SESSION STATE ─────────────────────────────────────────────────────────────
if "actions_df" not in st.session_state:
    st.session_state.actions_df = None
if "gen_df" not in st.session_state:
    st.session_state.gen_df = None

def load_data():
    actions_df = load_parquet("planning_rh/actions.parquet", ACTIONS_COLS)
    gen_df     = load_parquet("planning_rh/generateur.parquet", GENERATEUR_COLS)

    # Tente de créer les fichiers dans R2 s'ils n'existent pas encore
    # Ne bloque pas l'app si R2 est inaccessible
    if actions_df.empty:
        try:
            save_parquet(actions_df, "planning_rh/actions.parquet")
        except Exception as e:
            st.warning(f"⚠️ Impossible de créer actions.parquet dans R2 : {e}")
    if gen_df.empty:
        try:
            save_parquet(gen_df, "planning_rh/generateur.parquet")
        except Exception as e:
            st.warning(f"⚠️ Impossible de créer generateur.parquet dans R2 : {e}")

    st.session_state.actions_df = actions_df
    st.session_state.gen_df     = gen_df

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
        ["🏠 Tableau de bord", "➕ Nouvelle action", "✏️ Gérer les actions", "📅 Planning"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    if st.button("🔄 Recharger les données", use_container_width=True):
        load_data()
        st.success("Données rechargées.")

if st.session_state.actions_df is None:
    load_data()

actions_df = st.session_state.actions_df
gen_df     = st.session_state.gen_df

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
# PAGE: PLANNING
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

        col1, col2, col3, col4 = st.columns(4)
        with col1: f_statut = st.multiselect("Statut", STATUTS, default=["En retard", "En cours", "À venir"])
        with col2:
            types_dispo = gen["type"].dropna().unique().tolist()
            f_type = st.multiselect("Type", types_dispo, default=types_dispo)
        with col3:
            resps_dispo = gen["responsable"].dropna().unique().tolist()
            f_resp = st.multiselect("Responsable", resps_dispo, default=resps_dispo)
        with col4:
            mois_dispo  = sorted(gen["date_occurrence"].dropna().apply(lambda d: d.replace(day=1)).unique())
            mois_labels = {m: m.strftime("%B %Y") for m in mois_dispo}
            f_mois = st.multiselect("Mois", options=list(mois_labels.keys()),
                                    format_func=lambda m: mois_labels[m],
                                    default=list(mois_labels.keys()))

        filtered = gen[
            gen["statut"].isin(f_statut) &
            gen["type"].isin(f_type) &
            gen["responsable"].isin(f_resp) &
            gen["date_occurrence"].apply(lambda d: d.replace(day=1) if pd.notna(d) else None).isin(f_mois)
        ].sort_values(["deadline", "nom_action"])

        st.markdown(f"**{len(filtered)} occurrence(s)** affichée(s)")

        badge_map = {"À venir": "badge-avenir", "En cours": "badge-encours", "En retard": "badge-retard", "Fait": "badge-fait"}
        prio_map  = {"Basse": ("●","prio-basse"), "Moyenne": ("●","prio-moyenne"), "Haute": ("●","prio-haute"), "Critique": ("●","prio-critique")}

        rows_html = ""
        for _, r in filtered.iterrows():
            cls = badge_map.get(r["statut"], "badge-avenir")
            occ = r["date_occurrence"].strftime("%d/%m/%Y") if pd.notna(r["date_occurrence"]) else "—"
            dl  = r["deadline"].strftime("%d/%m/%Y") if pd.notna(r["deadline"]) else "—"
            prio_val = ""
            if not actions_df.empty:
                act_row = actions_df[actions_df["id"] == r["id_action"]]
                if not act_row.empty:
                    p = act_row.iloc[0].get("priorite", "")
                    dot, pcls = prio_map.get(p, ("", ""))
                    prio_val = f'<span class="{pcls}">{dot} {p}</span>'
            rows_html += f"""
            <tr>
                <td style="font-family:'DM Mono',monospace;font-size:11px;color:#8890a8">#{r['id_action']}</td>
                <td><b>{r['nom_action']}</b></td><td>{r['type']}</td>
                <td>{occ}</td><td>{dl}</td><td>{r['responsable']}</td>
                <td>{prio_val}</td>
                <td><span class="badge {cls}">{r['statut']}</span></td>
            </tr>"""

        st.markdown(f"""
        <div class="card" style="padding:0;overflow-x:auto;overflow:hidden">
        <table class="planning-table">
            <thead><tr><th>ID</th><th>Action</th><th>Type</th><th>Occurrence</th><th>Deadline</th><th>Responsable</th><th>Priorité</th><th>Statut</th></tr></thead>
            <tbody>{rows_html}</tbody>
        </table></div>""", unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("### Marquer comme Fait")
        col1, col2 = st.columns([3, 1])
        with col1:
            pending = filtered[filtered["statut"] != "Fait"]
            if not pending.empty:
                options = pending.apply(
                    lambda r: f"#{r['id_action']} — {r['nom_action']} ({r['date_occurrence'].strftime('%m/%Y') if pd.notna(r['date_occurrence']) else '?'})",
                    axis=1
                ).tolist()
                selected_occ = st.selectbox("Sélectionner une occurrence", options)
        with col2:
            if not pending.empty and st.button("✅ Marquer Fait", use_container_width=True):
                idx_sel = options.index(selected_occ)
                target  = pending.iloc[idx_sel]
                mask = (
                    (st.session_state.gen_df["id_action"] == target["id_action"]) &
                    (pd.to_datetime(st.session_state.gen_df["date_occurrence"]).dt.date == target["date_occurrence"])
                )
                st.session_state.gen_df.loc[mask, "date_traitement"] = datetime.now()
                st.session_state.gen_df.loc[mask, "statut"] = "Fait"
                try:
                    save_parquet(st.session_state.gen_df, "planning_rh/generateur.parquet")
                    st.success("✅ Occurrence marquée comme Fait.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erreur R2 : {e}")
