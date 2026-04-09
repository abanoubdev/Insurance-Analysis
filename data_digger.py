import pandas as pd
import re
from tabulate import tabulate
import time
import ssl

# Bypassing SSL for potential external lookups
ssl._create_default_https_context = ssl._create_unverified_context


def categorize_activity(val):
    """Categorizes raw activity strings into broad groups for hypothesis testing."""
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
    return None


def clean_age(val):
    val = str(val).lower()
    if 'teen' in val: return 15
    nums = re.findall(r'\d+', val)
    if nums: return int(nums[0])
    return None


def load_file_and_clean(url):
    print("Connecting to database and fetching records...")

    df = pd.read_excel(url)

    time.sleep(1)
    print("\nData loaded successfully.")

    # 1. Age Cleaning & Row Removal (CRITICAL: Dropping Nulls)
    df['Age_Clean'] = df['Age'].apply(clean_age)
    initial_count = len(df)
    df = df.dropna(subset=['Age_Clean']).copy()
    age_drop_count = initial_count - len(df)

    # 2. General Column Cleaning
    df['Is_Fatal'] = df['Fatal Y/N'].apply(clean_fatal)

    # Clean Country
    df['Country'] = df['Country'].str.strip().str.upper().fillna('UNKNOWN')

    # Clean State (using correct 'State' header from your dataset)
    df['State_Clean'] = df['State'].str.strip().str.title().fillna('Unknown')

    # Clean Type
    df['Type_Clean'] = df['Type'].str.strip().str.replace('Boat', 'Boating').fillna('Questionable')

    # Clean Activity (Mapping to Categories)
    df['Activity_Clean'] = df['Activity'].apply(categorize_activity)

    # 3. Year Filtering (1900 - 2026)
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    df = df[(df['Year'] >= 1900) & (df['Year'] <= 2026)].copy()

    # 4. Dropping noise/metadata columns
    cols_to_drop = [
        'Unnamed: 22', 'Unnamed: 21', 'original order', 'Case Number',
        'Case Number.1', 'href', 'href formula', 'pdf'
    ]
    df = df.drop(columns=cols_to_drop, errors='ignore')

    # 5. Duplicate Removal (Performed after cleaning to catch logically identical rows)
    pre_dup_count = len(df)
    df = df.drop_duplicates().copy()
    duplicates_removed = pre_dup_count - len(df)

    print(f"--- Data Cleaning Report ---")
    print(f"- Rows removed due to null Age: {age_drop_count}")
    print(f"- Duplicate records removed: {duplicates_removed}")
    print(f"- Final record count for analysis: {len(df)}")

    return df


def run_summary(df):
    print("\n" + "=" * 80)
    print("       GLOBAL SHARK ATTACK RISK ANALYSIS: EXECUTIVE SUMMARY       ")
    print("=" * 80)
    time.sleep(1)

    # Global Metrics
    print(f"OVERALL MARKET SCOPE (Validated Records):")
    print(f"- Total Unique Incidents: {len(df):,}")
    fatality_rate = df['Is_Fatal'].mean()
    print(f"- Global Avg. Fatality Rate: {fatality_rate:.2%}")
    print("-" * 80)

    # Table 1: Top 10 Countries (Newly Added)
    print("\n[TABLE 1] TOP 10 HIGH-INCIDENT COUNTRIES")
    country_counts = df['Country'].value_counts().head(10).reset_index()
    country_counts.columns = ['Country', 'Incident Count']
    print(tabulate(country_counts, headers='keys', tablefmt='fancy_grid', showindex=False))

    # Table 2: Top 10 States/Areas
    print("\n[TABLE 2] TOP 10 HIGH-INCIDENT STATES/AREAS")
    state_counts = df['State_Clean'].value_counts().head(10).reset_index()
    state_counts.columns = ['State/Area', 'Incident Count']
    print(tabulate(state_counts, headers='keys', tablefmt='fancy_grid', showindex=False))

    # Table 3: Activity Risk Profile
    print("\n[TABLE 3] ACTIVITY RISK PROFILE (CATEGORIZED)")
    activity_analysis = df.groupby('Activity_Clean')['Is_Fatal'].agg(['count', 'mean'])
    activity_analysis = activity_analysis.sort_values(by='count', ascending=False).reset_index()
    activity_analysis.columns = ['Category', 'Volume', 'Fatality Rate']
    activity_analysis['Fatality Rate'] = activity_analysis['Fatality Rate'].map(
        lambda x: f"{x:.2%}" if pd.notnull(x) else "0.00%")
    print(tabulate(activity_analysis, headers='keys', tablefmt='fancy_grid', showindex=False))

    # Table 4: Incident Type Distribution
    print("\n[TABLE 4] INCIDENT TYPE ANALYSIS")
    type_analysis = df['Type_Clean'].value_counts().reset_index()
    type_analysis.columns = ['Incident Type', 'Count']
    print(tabulate(type_analysis, headers='keys', tablefmt='fancy_grid', showindex=False))

    # Final Insights
    print("-" * 80)
    print("ANALYSIS READY FOR HYPOTHESIS TESTING:")
    print(f"1. Demographic: Avg. Age of victims is {df['Age_Clean'].mean():.1f} years.")
    print(f"2. Regional: {country_counts.iloc[0]['Country']} remains the highest risk market.")
    print(f"3. Integrity: All Null Ages and Duplicate records have been purged.")