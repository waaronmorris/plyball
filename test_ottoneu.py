from plyball.ottoneu import Ottoneu
import pandas as pd
from datetime import datetime
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Create output directory if it doesn't exist
output_dir = 'test_output'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Generate timestamp for file names
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Initialize Ottoneu client
client = Ottoneu(league_id=186)

# Get lineup for the specified team and date
lineup_df = client.get_line_up(
    team_id=1504,
    date="2025-06-22",
    stat_filter="season"
)

# Save raw data
raw_file = os.path.join(output_dir, f'lineup_raw_{timestamp}.csv')
lineup_df.to_csv(raw_file, index=False)
print(f"\nRaw data saved to: {raw_file}")

# Display the results
print("\nLineup Data:")
print("============")
print(f"Total players: {len(lineup_df)}")

# Display batter and pitcher counts
batter_count = lineup_df[lineup_df['is_position_player'] == True].shape[0]
pitcher_count = lineup_df[lineup_df['is_pitcher'] == True].shape[0]
print(f"\nBatters: {batter_count}")
print(f"Pitchers: {pitcher_count}")

# Process and display batters
print("\nBatters with Stats:")
print("==================")
batters = lineup_df[
    (lineup_df['is_position_player'] == True) & 
    (lineup_df['name'].notna())
].copy()

if not batters.empty:
    print("\nBatter Details:")
    batter_details = batters[[
        'name', 'position', 'player_positions', 'salary',
        'game_status', 'opponent_pitcher', 'opponent_pitcher_hand'
    ]]
    print(batter_details.to_string())
    
    # Save batter details
    batter_file = os.path.join(output_dir, f'batters_{timestamp}.csv')
    batter_details.to_csv(batter_file, index=False)
    print(f"\nBatter details saved to: {batter_file}")

# Process and display pitchers
print("\nPitchers with Stats:")
print("==================")
pitchers = lineup_df[
    (lineup_df['is_pitcher'] == True) & 
    (lineup_df['name'].notna())
].copy()

if not pitchers.empty:
    print("\nPitcher Details:")
    pitcher_details = pitchers[[
        'name', 'position', 'salary', 'game_status',
        'pitch_count_last_5_days', 'pitch_count_last_3_days'
    ]]
    print(pitcher_details.to_string())
    
    # Save pitcher details
    pitcher_file = os.path.join(output_dir, f'pitchers_{timestamp}.csv')
    pitcher_details.to_csv(pitcher_file, index=False)
    print(f"\nPitcher details saved to: {pitcher_file}")

# Create a summary file
summary = {
    'Total Players': len(lineup_df),
    'Batters': batter_count,
    'Pitchers': pitcher_count,
    'Active Batters': len(batters[~batters['position'].isin(['Bench', 'Minors', 'IL'])]),
    'Active Pitchers': len(pitchers[~pitchers['position'].isin(['Bench', 'Minors', 'IL'])]),
    'Bench Players': len(lineup_df[lineup_df['position'] == 'Bench']),
    'Minor League Players': len(lineup_df[lineup_df['position'] == 'Minors']),
    'IL Players': len(lineup_df[lineup_df['position'] == 'IL'])
}
summary_df = pd.DataFrame([summary]).T
summary_df.columns = ['Count']
summary_file = os.path.join(output_dir, f'summary_{timestamp}.csv')
summary_df.to_csv(summary_file)
print(f"\nSummary statistics saved to: {summary_file}") 