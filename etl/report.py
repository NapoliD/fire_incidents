import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

# Logging configuration
typelog = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=typelog)
logger = logging.getLogger(__name__)

# Connection and output parameters
DB_URI = os.getenv(
    "FIRE_DW_DB_URI",
    "postgresql://postgres:postgres@localhost:5432/sf_fire_incidents"
)
REPORT_PATH = os.getenv("REPORT_PATH", "validation_report.xlsx")


def connect_db(uri: str = None):
    """Creates and returns the SQLAlchemy engine."""
    uri = uri or DB_URI
    if not uri:
        raise ValueError("Database URI not configured")
    return create_engine(uri)


def generate_validation_metrics(engine):
    """Executes validation queries and generates descriptive statistical analysis with visualizations."""
    # Correction: use a specific seaborn style
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams['figure.figsize'] = (12, 8)
    plt.rcParams['font.size'] = 10
    
    """Executes validation queries and returns multiple DataFrames with descriptive metrics."""
    summary = []
    nulls = []
    battalion_df = None
    district_df = None

    with engine.connect() as conn:
        # 1. General count and staging vs fact ratio
        stg = conn.execute(text("SELECT COUNT(*) FROM stg_fire_incident")).scalar()
        fact = conn.execute(text("SELECT COUNT(*) FROM fact_fire_incident")).scalar()
        summary.append({'metric': 'Count staging', 'value': stg})
        summary.append({'metric': 'Count fact', 'value': fact})
        summary.append({'metric': 'Difference (stg - fact)', 'value': stg - fact})
        ratio = stg / fact if fact else None
        summary.append({'metric': 'Ratio staging/fact', 'value': f"{ratio:.2f}" if ratio is not None else 'N/A'})

        # 2. Date range in staging
        date_min = conn.execute(text("SELECT MIN(incident_datetime)::date FROM stg_fire_incident")).scalar()
        date_max = conn.execute(text("SELECT MAX(incident_datetime)::date FROM stg_fire_incident")).scalar()
        summary.append({'metric': 'Date range', 'value': f"{date_min} to {date_max}"})

        # 3. Duplicate detection
        dup = conn.execute(text(
            "SELECT COUNT(*) FROM ("
            " SELECT incident_number FROM stg_fire_incident"
            " GROUP BY incident_number HAVING COUNT(*) > 1) d"
        )).scalar()
        summary.append({'metric': 'Duplicate incident_id count', 'value': dup})

        # 4. Column statistics
        cols = [c[0] for c in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name='stg_fire_incident'")
        ).fetchall()]

        for col in cols:
            null_count = conn.execute(text(f"SELECT COUNT(*) FROM stg_fire_incident WHERE {col} IS NULL")).scalar()
            distinct_count = conn.execute(text(f"SELECT COUNT(DISTINCT {col}) FROM stg_fire_incident")).scalar()
            pct_null = (null_count / stg * 100) if stg else 0
            nulls.append({'column': col, 'null_count': null_count, 'null_pct': f"{pct_null:.2f}%", 'distinct_count': distinct_count})

        # 5. Top 5 distributions
        battalion_df = pd.read_sql(
            "SELECT battalion AS category, COUNT(*) AS count"
            " FROM stg_fire_incident GROUP BY battalion ORDER BY count DESC LIMIT 5;",
            conn
        )
        district_df = pd.read_sql(
            "SELECT supervisor_district AS category, COUNT(*) AS count"
            " FROM stg_fire_incident GROUP BY supervisor_district ORDER BY count DESC LIMIT 5;",
            conn
        )

    # Temporal and geographic analysis
    with engine.connect() as conn:  # Reopen connection
        temporal_df = pd.read_sql("""
            SELECT DATE_TRUNC('month', incident_datetime) as month,
                   COUNT(*) as incidents,
                   SUM(property_loss) as total_loss,
                   AVG(number_units_responding) as avg_units
            FROM stg_fire_incident
            GROUP BY month
            ORDER BY month;
        """, conn)
        
        geo_stats = pd.read_sql("""
            SELECT 
                neighborhood_district,
                COUNT(*) as incidents,
                AVG(property_loss) as avg_loss,
                SUM(civilian_injuries + firefighter_injuries) as total_injuries
            FROM stg_fire_incident
            WHERE neighborhood_district IS NOT NULL
            GROUP BY neighborhood_district
            ORDER BY incidents DESC;
        """, conn)
        
        severity_stats = pd.read_sql("""
            SELECT 
                CASE 
                    WHEN property_loss = 0 THEN 'No losses'
                    WHEN property_loss < 1000 THEN 'Low'
                    WHEN property_loss < 10000 THEN 'Medium'
                    ELSE 'High'
                END as severity_level,
                COUNT(*) as count
            FROM stg_fire_incident
            GROUP BY severity_level
            ORDER BY count DESC;
        """, conn)

    # Additional analysis of temporal patterns
    with engine.connect() as conn:
        # Analysis by hour of day
        hourly_pattern = pd.read_sql("""
            SELECT EXTRACT(HOUR FROM incident_datetime) as hour,
                   COUNT(*) as incidents
            FROM stg_fire_incident
            GROUP BY hour
            ORDER BY hour;
        """, conn)
        
        # Analysis by day of week
        weekly_pattern = pd.read_sql("""
            SELECT TO_CHAR(incident_datetime, 'Day') as day_of_week,
                   COUNT(*) as incidents
            FROM stg_fire_incident
            GROUP BY day_of_week
            ORDER BY COUNT(*) DESC;
        """, conn)
        
        # Response and damage analysis
        response_analysis = pd.read_sql("""
            SELECT 
                AVG(number_units_responding) as avg_units,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY property_loss) as median_loss,
                AVG(CASE WHEN civilian_injuries > 0 OR firefighter_injuries > 0 THEN 1 ELSE 0 END) as injury_rate
            FROM stg_fire_incident;
        """, conn)

    # Generate visualizations
    figs = {}
    
    # 1. Temporal trend of incidents
    fig_temporal = plt.figure()
    plt.plot(temporal_df['month'], temporal_df['incidents'], marker='o')
    plt.title('Fire Incidents Trend by Month')
    plt.xlabel('Month')
    plt.ylabel('Number of Incidents')
    plt.xticks(rotation=45)
    plt.tight_layout()
    figs['temporal_trend'] = fig_temporal

    # 2. Geographic distribution
    fig_geo = plt.figure()
    sns.barplot(data=geo_stats.head(10), x='incidents', y='neighborhood_district')
    plt.title('Top 10 Districts by Number of Incidents')
    plt.xlabel('Number of Incidents')
    plt.ylabel('District')
    plt.tight_layout()
    figs['geographic_dist'] = fig_geo

    # 3. Severity levels
    fig_severity = plt.figure()
    plt.pie(severity_stats['count'], labels=severity_stats['severity_level'], autopct='%1.1f%%')
    plt.title('Distribution of Incidents by Severity Level')
    plt.axis('equal')
    figs['severity_dist'] = fig_severity

    # 4. Correlation between responding units and losses
    fig_correlation = plt.figure()
    with engine.connect() as conn:  # Reopen connection
        sns.scatterplot(data=pd.read_sql('SELECT number_units_responding, property_loss FROM stg_fire_incident', conn),
                        x='number_units_responding', y='property_loss')
    plt.title('Correlation between Responding Units and Material Losses')
    plt.xlabel('Number of Units')
    plt.ylabel('Losses ($)')
    plt.tight_layout()
    figs['correlation'] = fig_correlation

    # 5. Patterns by hour of day
    fig_hourly = plt.figure()
    plt.bar(hourly_pattern['hour'], hourly_pattern['incidents'])
    plt.title('Distribution of Incidents by Hour of Day')
    plt.xlabel('Hour')
    plt.ylabel('Number of Incidents')
    plt.xticks(range(0, 24))
    plt.tight_layout()
    figs['hourly_pattern'] = fig_hourly

    # 6. Patterns by day of week
    fig_weekly = plt.figure()
    sns.barplot(data=weekly_pattern, x='day_of_week', y='incidents')
    plt.title('Distribution of Incidents by Day of Week')
    plt.xlabel('Day')
    plt.ylabel('Number of Incidents')
    plt.xticks(rotation=45)
    plt.tight_layout()
    figs['weekly_pattern'] = fig_weekly

    # Add response metrics to summary
    summary.append({'metric': 'Average units per incident', 'value': f"{response_analysis['avg_units'].iloc[0]:.2f}"})
    summary.append({'metric': 'Median loss per incident', 'value': f"${response_analysis['median_loss'].iloc[0]:.2f}"})
    summary.append({'metric': 'Injury rate in incidents', 'value': f"{response_analysis['injury_rate'].iloc[0]*100:.1f}%"})

    # Create final DataFrames
    summary_df = pd.DataFrame(summary)
    nulls_df = pd.DataFrame(nulls)

    return summary_df, nulls_df, battalion_df, district_df, temporal_df, geo_stats, severity_stats, figs


def save_report(summary_df, nulls_df, battalion_df, district_df, temporal_df, geo_stats, severity_stats, figs, path: str = REPORT_PATH):
    """Saves metrics in separate Excel sheets."""
    # Save data to Excel
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        nulls_df.to_excel(writer, sheet_name='Nulls', index=False)
        battalion_df.to_excel(writer, sheet_name='Top 5 Battalions', index=False)
        district_df.to_excel(writer, sheet_name='Top 5 Districts', index=False)
        temporal_df.to_excel(writer, sheet_name='Temporal Analysis', index=False)
        geo_stats.to_excel(writer, sheet_name='Geographic Statistics', index=False)
        severity_stats.to_excel(writer, sheet_name='Severity Levels', index=False)
    
    # Save visualizations
    report_dir = os.path.dirname(path)
    for name, fig in figs.items():
        fig.savefig(os.path.join(report_dir, f'fire_incidents_{name}.png'))
        plt.close(fig)
    
    logger.info(f"Validation report saved at: {path}")
    logger.info(f"Visualizations saved in: {report_dir}")


if __name__ == '__main__':
    engine = connect_db()
    summary_df, nulls_df, battalion_df, district_df, temporal_df, geo_stats, severity_stats, figs = generate_validation_metrics(engine)
    save_report(summary_df, nulls_df, battalion_df, district_df, temporal_df, geo_stats, severity_stats, figs)
    print("Validation report generated with statistical analysis and visualizations:")
    print(summary_df)