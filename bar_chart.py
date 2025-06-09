import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

risk_profiles_1 = [
  {
    'id': 1,
    'name': 'Conservative A',
    'bonds_aaa_a': 0.3,
    'bonds_bbb': 0.7,
    'bonds_bb': 0,
    'etfs': 0,
    'average_yield': .06103,
    'min_score': 0,
    'max_score': 0.9
  },
  {
    'id': 2,
    'name': 'Conservative B',
    'bonds_aaa_a': 0.18,
    'bonds_bbb': 0.54,
    'bonds_bb': .18,
    'etfs': .1,
    'average_yield': .0723,
    'min_score': 0.9,
    'max_score': 1.25
  },
  {
    'id': 3,
    'name': 'Moderate A',
    'bonds_aaa_a': 0.16,
    'bonds_bbb': 0.48,
    'bonds_bb': 0.16,
    'etfs': 0.2,
    'average_yield': .0764,
    'min_score': 1.25,
    'max_score': 1.5
  },
  {
    'id': 4,
    'name': 'Moderate B',
    'bonds_aaa_a': 0.15,
    'bonds_bbb': 0.375,
    'bonds_bb': 0.15,
    'etfs': 0.25,
    'average_yield': .0736,
    'min_score': 1.5,
    'max_score': 2
  },
  {
    'id': 5,
    'name': 'Moderate C',
    'bonds_aaa_a': 0.14,
    'bonds_bbb': 0.35,
    'bonds_bb': 0.21,
    'etfs': 0.3,
    'average_yield': .06103,
    'min_score': 2,
    'max_score': 2.5
  },
  {
    'id': 6,
    'name': 'Aggressive A',
    'bonds_aaa_a': 0.13,
    'bonds_bbb': 0.325,
    'bonds_bb': .195,
    'etfs': .35,
    'average_yield': .0845,
    'min_score': 2.5,
    'max_score': 2.75
  },
  {
    'id': 7,
    'name': 'Aggressive B',
    'bonds_aaa_a': 0.12,
    'bonds_bbb': 0.30,
    'bonds_bb': 0.18,
    'etfs': 0.4,
    'average_yield': .0865,
    'min_score': 2.75,
    'max_score': 3
  },
  {
    'id': 8,
    'name': 'Aggressive C',
    'bonds_aaa_a': 0.05,
    'bonds_bbb': 0.25,
    'bonds_bb': 0.20,
    'etfs': 0.5,
    'average_yield': .0925,
    'min_score': 3,
    'max_score': 10
  }
]

risk_profiles_2 = [
  {
    'id': 1,
    'name': 'Conservative A',
    'bonds_aaa_a': 0.36,
    'bonds_bbb': 0.64,
    'bonds_bb': 0,
    'etfs': 0,
    'average_yield': .06103,
    'min_score': 0,
    'max_score': 0.9
  },
  {
    'id': 2,
    'name': 'Conservative B',
    'bonds_aaa_a': 0.32,
    'bonds_bbb': 0.58,
    'bonds_bb': .1,
    'etfs': 0,
    'average_yield': .0723,
    'min_score': 0.9,
    'max_score': 1.25
  },
  {
    'id': 3,
    'name': 'Moderate A',
    'bonds_aaa_a': 0.28,
    'bonds_bbb': 0.52,
    'bonds_bb': 0.10,
    'etfs': 0.10,
    'average_yield': .0764,
    'min_score': 1.25,
    'max_score': 1.5
  },
  {
    'id': 4,
    'name': 'Moderate B',
    'bonds_aaa_a': 0.24,
    'bonds_bbb': 0.46,
    'bonds_bb': 0.2,
    'etfs': 0.1,
    'average_yield': .0736,
    'min_score': 1.5,
    'max_score': 2
  },
  {
    'id': 5,
    'name': 'Moderate C',
    'bonds_aaa_a': 0.20,
    'bonds_bbb': 0.40,
    'bonds_bb': 0.2,
    'etfs': 0.2,
    'average_yield': .06103,
    'min_score': 2,
    'max_score': 2.5
  },
  {
    'id': 6,
    'name': 'Aggressive A',
    'bonds_aaa_a': 0.16,
    'bonds_bbb': 0.34,
    'bonds_bb': 0.2,
    'etfs': 0.3,
    'average_yield': .0845,
    'min_score': 2.5,
    'max_score': 2.75
  },
  {
    'id': 7,
    'name': 'Aggressive B',
    'bonds_aaa_a': 0.12,
    'bonds_bbb': 0.28,
    'bonds_bb': 0.2,
    'etfs': 0.4,
    'average_yield': .0865,
    'min_score': 2.75,
    'max_score': 3
  },
  {
    'id': 8,
    'name': 'Aggressive C',
    'bonds_aaa_a': 0.08,
    'bonds_bbb': 0.22,
    'bonds_bb': 0.20,
    'etfs': 0.5,
    'average_yield': .0925,
    'min_score': 3,
    'max_score': 10
  }
]

# Calculate and print sum of allocations for each profile set
def print_allocation_sums(profiles_data, profile_set_name):
    print(f"Sum of asset allocations for {profile_set_name}:")
    for profile in profiles_data:
        allocation_sum = (
            profile.get('bonds_aaa_a', 0) +
            profile.get('bonds_bbb', 0) +
            profile.get('bonds_bb', 0) +
            profile.get('etfs', 0)
        )
        print(f"  {profile['name']}: {allocation_sum:.4f}")
    print("-"*40 + "\n")

print_allocation_sums(risk_profiles_1, "Risk Profiles Set 1")
print_allocation_sums(risk_profiles_2, "Risk Profiles Set 2")

asset_classes_labels = ['Bonds AAA-A', 'Bonds BBB', 'Bonds BB', 'ETFs']
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'] # Example colors

# Helper function to plot stacked bar chart
def plot_stacked_bar_chart(ax, profiles_data, chart_title):
    profile_names = [p['name'] for p in profiles_data]
    bonds_aaa_a = np.array([p['bonds_aaa_a'] for p in profiles_data])
    bonds_bbb = np.array([p['bonds_bbb'] for p in profiles_data])
    bonds_bb = np.array([p['bonds_bb'] for p in profiles_data])
    etfs_data = np.array([p['etfs'] for p in profiles_data]) # Renamed to avoid conflict

    data_to_stack = np.array([bonds_aaa_a, bonds_bbb, bonds_bb, etfs_data])
    bottom = np.zeros(len(profile_names))

    for i, asset_class_label in enumerate(asset_classes_labels):
        ax.bar(profile_names, data_to_stack[i], bottom=bottom, label=asset_class_label, color=colors[i])
        bottom += data_to_stack[i]

    ax.set_xlabel("Risk Profile Name")
    ax.set_ylabel("Proportion of Assets")
    ax.set_title(chart_title)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.grid(axis='y', linestyle='--', alpha=0.7)

# --- Create the first figure for side-by-side stacked bar charts --- #
fig1, (ax1, ax2) = plt.subplots(1, 2, figsize=(22, 8), sharey=True)
fig1.suptitle('Comparison of Asset Allocations for Two Profile Sets', fontsize=16)

plot_stacked_bar_chart(ax1, risk_profiles_1, 'Asset Allocation - Profile Set 1')
plot_stacked_bar_chart(ax2, risk_profiles_2, 'Asset Allocation - Profile Set 2')

# Add a single legend for the entire figure
handles, labels = ax1.get_legend_handles_labels()
fig1.legend(handles, labels, loc='upper right', title="Asset Class")

plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout for suptitle and legend
plt.show()


# --- Create a second figure for individual asset progression (using risk_profiles_1) --- #
# Extract data for plotting from risk_profiles_1
profile_names_set1 = [profile['name'] for profile in risk_profiles_1]
bonds_aaa_a_set1 = np.array([profile['bonds_aaa_a'] for profile in risk_profiles_1])
bonds_bbb_set1 = np.array([profile['bonds_bbb'] for profile in risk_profiles_1])
bonds_bb_set1 = np.array([profile['bonds_bb'] for profile in risk_profiles_1])
etfs_set1 = np.array([profile['etfs'] for profile in risk_profiles_1])

fig2, axs = plt.subplots(2, 2, figsize=(17, 12), sharey=True)
fig2.suptitle('Progression of Individual Asset Allocations Across Risk Profiles (Set 1)', fontsize=16)

asset_data_map_set1 = {
    asset_classes_labels[0]: bonds_aaa_a_set1,
    asset_classes_labels[1]: bonds_bbb_set1,
    asset_classes_labels[2]: bonds_bb_set1,
    asset_classes_labels[3]: etfs_set1
}

plot_positions = [(0, 0), (0, 1), (1, 0), (1, 1)]

for i, (asset_name, asset_values) in enumerate(asset_data_map_set1.items()):
    ax = axs[plot_positions[i]]
    ax.plot(profile_names_set1, asset_values, marker='o', linestyle='-', color=colors[i])
    ax.set_title(f'Progression of {asset_name}')
    ax.set_xlabel("Risk Profile Name")
    ax.set_ylabel("Proportion of Assets")
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    for j, val in enumerate(asset_values):
        ax.text(j, val + 0.01, f'{val:.2f}', ha='center', va='bottom', fontsize=8)

max_val_set1 = np.max(list(asset_data_map_set1.values()))
for ax_row in axs:
    for ax_col in ax_row:
        ax_col.set_ylim(0, max_val_set1 * 1.15 if max_val_set1 > 0 else 0.15) # Handle case where max_val is 0
        ax_col.set_yticks(np.arange(0, (max_val_set1 * 1.1) if max_val_set1 > 0 else 0.11, 0.1))

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()

dataframe = pd.DataFrame(data=risk_profiles_2)
dataframe.to_csv('risk_profiles_2.csv', index=False)