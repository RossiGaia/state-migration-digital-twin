class MigratePatternReplication():
    def __init__(self, processing_layer):
        self.base_processing = processing_layer

    def serialize_state(self):
        serialized_state = {
            "conveyor_params": self.base_processing.conveyor_params,
            "recv_buffer": list(self.base_processing.recv_buffer),
            "processing_buffer": list(self.base_processing.processing_buffer),
        }
        return serialized_state

    def deserialize_state(self):
        pass