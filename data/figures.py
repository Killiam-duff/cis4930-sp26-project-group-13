import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# Load Data
df = pd.read_csv("processed/batting_stats_cleaned.csv")

# Palette / style
sns.set_theme(style="whitegrid", palette="muted")
ACCENT   = "#1f77b4"
ACCENT2  = "#ff7f0e"
PALETTE  = {"Above Avg": "#2ca02c", "Below Avg": "#d62728"}

# Figure 1: Distribution of OPS (histogram + KDE)
fig, ax = plt.subplots(figsize=(9, 5))

sns.histplot(df["OPS"], bins=40, kde=True, color=ACCENT,
             edgecolor="white", linewidth=0.4, ax=ax)

mean_ops = df["OPS"].mean()
med_ops  = df["OPS"].median()
ax.axvline(mean_ops, color="red",    linestyle="--", linewidth=1.5,
           label=f"Mean  {mean_ops:.3f}")
ax.axvline(med_ops,  color="orange", linestyle=":",  linewidth=1.5,
           label=f"Median {med_ops:.3f}")

ax.set_title("Distribution of OPS Across All Player-Seasons", fontsize=14, fontweight="bold")
ax.set_xlabel("OPS (On-base Plus Slugging)", fontsize=11)
ax.set_ylabel("Count", fontsize=11)
ax.legend(fontsize=10)
plt.tight_layout()
plt.savefig("figures/fig1_ops_distribution.png", dpi=300)
plt.close()
print("Saved fig1_ops_distribution.png")

# Figure 2: Avg SB & Avg OPS by Age (dual-axis line chart)
age_grp = df.groupby("Age")[["SB", "OPS"]].mean().reset_index()
# Keep only ages with reasonable sample sizes (≥10 players)
age_counts = df.groupby("Age")["Player"].count()
valid_ages = age_counts[age_counts >= 10].index
age_grp = age_grp[age_grp["Age"].isin(valid_ages)]

fig, ax1 = plt.subplots(figsize=(10, 5))
ax2 = ax1.twinx()

ln1 = ax1.plot(age_grp["Age"], age_grp["SB"],  color=ACCENT,  marker="o",
               linewidth=2, label="Avg Stolen Bases")
ln2 = ax2.plot(age_grp["Age"], age_grp["OPS"], color=ACCENT2, marker="s",
               linewidth=2, linestyle="--", label="Avg OPS")

ax1.set_xlabel("Age", fontsize=11)
ax1.set_ylabel("Avg Stolen Bases", fontsize=11, color=ACCENT)
ax2.set_ylabel("Avg OPS",          fontsize=11, color=ACCENT2)
ax1.tick_params(axis="y", labelcolor=ACCENT)
ax2.tick_params(axis="y", labelcolor=ACCENT2)

lns  = ln1 + ln2
labs = [l.get_label() for l in lns]
ax1.legend(lns, labs, loc="upper right", fontsize=10)
ax1.set_title("Avg Stolen Bases & OPS by Player Age\n(ages with ≥10 player-seasons)",
              fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig("figures/fig2_age_sb_ops.png", dpi=300)
plt.close()
print("Saved fig2_age_sb_ops.png")

# Figure 3: Correlation Heat-map (HR, BB, BA, OPS, WAR, SO)
# Heat-map already in analysis, but we duplicate it here for presentation figures
cols = ["HR", "BB", "BA", "OPS", "WAR", "SO"]
corr = df[cols].corr()

fig, ax = plt.subplots(figsize=(7, 6))
mask = np.triu(np.ones_like(corr, dtype=bool), k=1)   # show full matrix
sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", center=0,
            linewidths=0.5, ax=ax, square=True,
            annot_kws={"size": 10})
ax.set_title("Correlation Matrix – Key Batting Statistics", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig("figures/fig3_correlation_heatmap.png", dpi=300)
plt.close()
print("Saved fig3_correlation_heatmap.png")

# Figure 4: Scatter HR vs WAR, coloured by OPS tier
fig, ax = plt.subplots(figsize=(9, 6))

for tier, grp in df.groupby("OPS_tier"):
    ax.scatter(grp["HR"], grp["WAR"], label=tier,
               color=PALETTE[tier], alpha=0.45, s=18, edgecolors="none")

ax.set_xlabel("Home Runs (HR)", fontsize=11)
ax.set_ylabel("Wins Above Replacement (WAR)", fontsize=11)
ax.set_title("Home Runs vs WAR  –  coloured by OPS Tier", fontsize=13, fontweight="bold")
ax.legend(title="OPS Tier", fontsize=10)
plt.tight_layout()
plt.savefig("figures/fig4_hr_vs_war_ops_tier.png", dpi=300)
plt.close()
print("Saved fig4_hr_vs_war_ops_tier.png")

# Figure 5: League comparison bar chart (avg BA, OPS, HR)
league_grp = (df[df["Lg"].isin(["AL", "NL"])]          # exclude "2LG" multi-league rows
              .groupby("Lg")[["BA", "OPS", "HR"]]
              .mean()
              .reset_index())

metrics = ["BA", "OPS", "HR"]
labels  = ["Batting Average", "OPS", "Home Runs"]
fig, axes = plt.subplots(1, 3, figsize=(11, 5))

for ax, metric, label in zip(axes, metrics, labels):
    bars = ax.bar(league_grp["Lg"], league_grp[metric],
                  color=[ACCENT, ACCENT2], width=0.5, edgecolor="white")
    ax.bar_label(bars, fmt="%.3f" if metric in ("BA", "OPS") else "%.1f",
                 padding=3, fontsize=10)
    ax.set_title(label, fontsize=12, fontweight="bold")
    ax.set_xlabel("League", fontsize=10)
    ax.set_ylim(0, league_grp[metric].max() * 1.18)
    ax.yaxis.set_major_formatter(
        mticker.FormatStrFormatter("%.3f") if metric in ("BA", "OPS") else mticker.FormatStrFormatter("%.1f")
    )

fig.suptitle("AL vs NL – Average Batting Statistics", fontsize=14, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig("figures/fig5_league_comparison.png", dpi=300, bbox_inches="tight")
plt.close()
print("Saved fig5_league_comparison.png")

# Figure 6: Box-plot OPS distribution by OPS tier
fig, ax = plt.subplots(figsize=(7, 5))
order = ["Above Avg", "Below Avg"]
sns.boxplot(data=df, x="OPS_tier", y="OPS", order=order,
            palette=PALETTE, width=0.45, linewidth=1.2,
            flierprops=dict(marker="o", markersize=3, alpha=0.3),
            ax=ax)
ax.axhline(df["OPS"].mean(), color="black", linestyle="--",
           linewidth=1.2, label=f"League mean ({df['OPS'].mean():.3f})")
ax.set_title("OPS Distribution by Tier", fontsize=13, fontweight="bold")
ax.set_xlabel("OPS Tier", fontsize=11)
ax.set_ylabel("OPS",      fontsize=11)
ax.legend(fontsize=10)
plt.tight_layout()
plt.savefig("figures/fig6_ops_boxplot_by_tier.png", dpi=300)
plt.close()
print("Saved fig6_ops_boxplot_by_tier.png")

print("All figures saved successfully.")