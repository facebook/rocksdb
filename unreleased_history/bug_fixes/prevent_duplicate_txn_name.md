Fix a race condition in pessimistic transactions that could allow multiple transactions with the same name to be registered simultaneously, resulting in a crash or other unpredictable behavior.
