import pandas as pd
import calendar

# -------------------------
# Seuils de température spécifiques par département
SEUILS_PAR_DEP = {
    "01": 17.0, "02": 16.0, "03": 16.0, "04": 16.0, "05": 15.0, "06": 17.5, "07": 16.5, "08": 16.0, "09": 16.0, "10": 16.0,
    "11": 17.0, "12": 16.0, "13": 18.0, "14": 15.0, "15": 16.0, "16": 17.0, "17": 16.0, "18": 15.0, "19": 16.0, "21": 16.0,
    "22": 15.5, "23": 14.5, "24": 16.0, "25": 15.5, "26": 16.5, "27": 16.0, "28": 15.0, "29": 14.5, "2A": 17.0, "2B": 17.0,
    "30": 17.0, "31": 16.0, "32": 16.5, "33": 16.5, "34": 17.0, "35": 15.5, "36": 15.0, "37": 16.0, "38": 16.0, "39": 15.5,
    "40": 17.0, "41": 16.0, "42": 16.0, "43": 16.0, "44": 15.0, "45": 16.0, "46": 16.0, "47": 16.5, "48": 16.0, "49": 16.0,
    "50": 15.0, "51": 16.0, "52": 15.5, "53": 16.0, "54": 17.0, "55": 16.0, "56": 15.5, "57": 16.0, "58": 15.5, "59": 17.5,
    "60": 16.0, "61": 15.0, "62": 16.0, "63": 16.0, "64": 17.0, "65": 16.0, "66": 17.0, "67": 16.0, "68": 16.5, "69": 17.0,
    "70": 15.5, "71": 16.0, "72": 16.0, "73": 16.0, "74": 16.0, "75": 16.5, "76": 16.0, "77": 16.0, "78": 16.0, "79": 16.0,
    "80": 17.0, "81": 16.0, "82": 16.0, "83": 18.0, "84": 17.0, "85": 16.0, "86": 16.0, "87": 14.5, "88": 16.0, "89": 15.5,
    "90": 16.0, "91": 16.0, "92": 16.0, "93": 16.0, "94": 16.0, "95": 16.0
}

# -------------------------
def prepare_meteo(df):
    df = df.copy()
    df['month'] = pd.to_datetime(df['date']).dt.month.astype(int)
    df['departement'] = df['departement'].astype(str).str.zfill(2)
    return df

def estimer_thermosensibilite(departement_code, annee, df_models):
    row = df_models[df_models['code_departement'] == departement_code]
    if row.empty:
        return None, f"Aucune régression trouvée pour le département {departement_code}."
    a = row['a_slope_thermosensibility'].values[0]
    b = row['b_intercept_thermosensibility'].values[0]
    r2 = row['r2_thermosensibility'].values[0]
    thermosensibilite = max(0, a * annee + b)
    return thermosensibilite, f"Thermosensibilité estimée = {thermosensibilite:.0f} kWh/DJU (r²={r2:.2f})"

def estimer_conso_complete_mensuelle(departement_code, annee, temp_moyenne_mois, df_models, nb_jours=31):
    code_str = str(departement_code).zfill(2)
    seuil = SEUILS_PAR_DEP.get(code_str, 16)  # valeur par défaut si absente
    thermo, info = estimer_thermosensibilite(departement_code, annee, df_models)
    if thermo is None:
        return None, info
    row = df_models[df_models['code_departement'] == departement_code]
    a = row['a_slope_non_thermosensible'].values[0]
    b = row['b_intercept_non_thermosensible'].values[0]
    dju_mois = max(0, seuil - temp_moyenne_mois) * nb_jours
    conso_thermo_mois = (thermo * dju_mois) / 1000
    conso_non_thermo_mois = (a * annee + b) / 12
    conso_totale_mois = conso_thermo_mois + conso_non_thermo_mois
    return conso_totale_mois, {
        "conso_thermo": round(conso_thermo_mois),
        "conso_non_thermo": round(conso_non_thermo_mois),
        "total": round(conso_totale_mois),
        "temp_corrigee": round(temp_moyenne_mois, 1),
        "seuil": seuil
    } 


def consommation_annuelle_par_departement(annee_cumule, df_models, df_meteo_indexed, temp_offset=0):
    result = []
    departements = df_models['code_departement'].unique()

    for code in sorted(departements):
        total = 0
        for month in range(1, 13):
            code_str = f"{code:02d}"
            temp = df_meteo_indexed.loc[(code_str, month), 'temperature'].mean()
            temp_adjusted = temp + temp_offset
            nb_jours = calendar.monthrange(annee_cumule, month)[1]
            estimation = estimer_conso_complete_mensuelle(code, annee_cumule, temp_adjusted, df_models, nb_jours=nb_jours)
            if isinstance(estimation, tuple):
                conso, _ = estimation
                if pd.notna(conso):
                    total += conso

        result.append({
            "code_departement": code,
            "conso_totale_MWh": round(total, 0)
        })

    return pd.DataFrame(result)

