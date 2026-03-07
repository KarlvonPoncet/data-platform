import json
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class StateManager:
    """
    Manages the high-water marks for transformation pipelines to avoid expensive S3 scans.
    Uses a local JSON file to persist the last processed timestamps for different stages.
    """
    def __init__(self, state_file_path: str = "pipeline_state.json"):
        self.state_file_path = os.getenv("STATE_FILE_PATH", state_file_path)
        self._state = self._load_state()

    def _load_state(self) -> dict:
        """Loads state from the JSON file. Returns empty dict if file doesn't exist."""
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode state file: {e}. Starting with empty state.")
                return {}
            except Exception as e:
                logger.error(f"Error reading state file {self.state_file_path}: {e}")
                return {}
        return {}

    def _save_state(self):
        """Persists the current state to the JSON file."""
        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(self._state, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save state to {self.state_file_path}: {e}")

    def get_last_processed_timestamp(self, pipeline_stage: str) -> str:
        """
        Retrieves the last processed timestamp string for a given pipeline stage.
        Returns None if the stage hasn't been processed yet.
        """
        return self._state.get(pipeline_stage)

    def set_last_processed_timestamp(self, pipeline_stage: str, timestamp_str: str):
        """
        Updates the last processed timestamp for a pipeline stage and saves to disk.
        """
        self._state[pipeline_stage] = timestamp_str
        self._save_state()
        logger.info(f"Updated state for '{pipeline_stage}' to {timestamp_str}")
