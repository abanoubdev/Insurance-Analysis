import pandas as pd
import re
from tabulate import tabulate
import time
import ssl

# Bypassing SSL for direct download if needed
ssl._create_default_https_context = ssl._create_unverified_context


def categorize_activity(val):
    val = str(val).lower()
    if 'surf' in val: return 'Surfing'
    if 'swim' in val or 'bath' in val: return 'Swimming'
    if 'fish' in val: return 'Fishing'
    if 'dive' in val or 'snorkel' in val: return 'Diving'
    if 'board' in val: return 'Board Sports'
    return 'Other/Unknown'


def clean_fatal(val):
    val = str(val).strip().upper()
    if val == 'Y': return 1
    if val in ['N', 'N ', ' N']: return 0
    return 'N'


def clean_age(val):
    val = str(val).lower()
    if 'teen' in val: return 15
    nums = re.findall(r'\d+', val)
    if nums: return int(nums[0])
    return None


def load_file_and_clean(url):
    print("Connecting to database and fetching latest records...")

    # Load data
    df = pd.read_excel(url, engine='xlrd')
    time.sleep(1)
    print("\nWohoo, Data is fetched successfully !!")

    df['Age_Clean'] = df['Age'].apply(clean_age)

    df = df.dropna(subset=['Age_Clean']).copy()

    df['Is_Fatal'] = df['Fatal Y/N'].apply(clean_fatal)
    df['Age_Clean'] = df['Age'].apply(clean_age)
    df['Country'] = df['Country'].str.strip().str.upper()

    df['State_Clean'] = df['Area'].str.strip().str.title().fillna('Unknown')

    df['Type_Clean'] = df['Type'].str.strip().str.replace('Boat', 'Boating').fillna('Questionable')

    df['Activity_Clean'] = df['Activity'].apply(categorize_activity)

    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    df = df[(df['Year'] >= 1900) & (df['Year'] <= 2026)].copy()

    cols_to_drop = ['Unnamed: 22', 'Unnamed: 21', 'original order', 'Case Number',
                    'Case Number.1', 'href', 'href formula', 'pdf']
    df = df.drop(columns=cols_to_drop, errors='ignore')
    df = df.drop_duplicates().copy()

    return df


def run_summary(df):
    print("\n" + "=" * 80)
    print("       GLOBAL SHARK ATTACK RISK ANALYSIS: EXECUTIVE SUMMARY       ")
    print("=" * 80)
    time.sleep(1)

    # Global Metrics
    print(f"OVERALL MARKET SCOPE:")
    print(f"- Total Validated Incidents: {len(df):,}")
    fatality_rate = df['Is_Fatal'].mean()
    print(f"- Global Avg. Fatality Rate: {fatality_rate:.2%}")
    print("-" * 80)

    # Table 1: Regional Risk (States/Areas)
    print("\n[TABLE 1] TOP 10 HIGH-INCIDENT STATES/AREAS")
    state_counts = df['State_Clean'].value_counts().head(10).reset_index()
    state_counts.columns = ['State/Area', 'Incident Count']
    print(tabulate(state_counts, headers='keys', tablefmt='fancy_grid', showindex=False))

    # Table 2: Activity Risk Profile (Using New Categories)
    print("\n[TABLE 2] ACTIVITY RISK PROFILE (CATEGORIZED)")
    activity_analysis = df.groupby('Activity_Clean')['Is_Fatal'].agg(['count', 'mean'])
    activity_analysis = activity_analysis.sort_values(by='count', ascending=False).reset_index()
    activity_analysis.columns = ['Category', 'Volume', 'Fatality Rate']
    activity_analysis['Fatality Rate'] = activity_analysis['Fatality Rate'].map(lambda x: f"{x:.2%}")
    print(tabulate(activity_analysis, headers='keys', tablefmt='fancy_grid', showindex=False))

    # Table 3: Incident Type Distribution
    print("\n[TABLE 3] INCIDENT TYPE ANALYSIS")
    type_analysis = df['Type_Clean'].value_counts().reset_index()
    type_analysis.columns = ['Incident Type', 'Count']
    print(tabulate(type_analysis, headers='keys', tablefmt='fancy_grid', showindex=False))

    # Recommendations
    print("-" * 80)
    print("INSURANCE RECOMMENDATIONS & RISK INSIGHTS:")
    print("1. Apply 'High-Severity' surcharges for activities with Fatality Rates > 20%.")
    print(f"2. Geographic Focus: {state_counts.iloc[0]['State/Area']} accounts for the highest regional volume.")
    print(f"3. High-risk age demographic: {df[df['Age_Clean'] <= 19]['Age_Clean'].count()} incidents involve minors.")
    print("4. Hypothesis Check: Activity and Type are now cleaned for correlation testing.")