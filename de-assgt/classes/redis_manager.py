"""
-------------------------
Author: Kelly Tan Jie Li|
-------------------------

"""

import redis
import csv

class RedisManager:
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

    @staticmethod
    def generate_key(word):
        if not word:
            return None
        
        vowels = 'aeiou'
        if len(word) <= 3:
            return word
        
        first_letter = word[0]
        rest_of_word = ''.join([c for c in word[1:] if c not in vowels])
        key = first_letter + rest_of_word
        return key

    @staticmethod
    def check_or_assign_key(word):
        if not word:
            return None

        # Check if a key for the word already exists in Redis
        redis_key = f"word_to_key:{word}"
        assigned_key = RedisManager.redis_client.get(redis_key)
        
        if assigned_key:
            return assigned_key  
        
        generated_key = RedisManager.generate_key(word)
        if not generated_key:
            return None
        
        sequence = RedisManager.redis_client.incr("key_sequence")
        unique_key = f"{generated_key}_{sequence}"

        RedisManager.redis_client.set(redis_key, unique_key)

        return unique_key

    @staticmethod
    def store_data_to_redis(input_csv):
        redis_client = RedisManager.redis_client

        with open(input_csv, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t')

            for row in reader:
                word = row[reader.fieldnames[0]] 
                pos_tag = row.get('pos_tag', '')  
                try:
                    redis_key = RedisManager.check_or_assign_key(word)

                    if redis_client.exists(redis_key):
                        # If the key exists, append the pos_tag value to the pos_tag field
                        existing_pos_tags = redis_client.hget(redis_key, 'pos_tag') or ''

                        if pos_tag and pos_tag not in existing_pos_tags.split(','):
                            updated_pos_tags = ','.join(filter(None, [existing_pos_tags, pos_tag]))
                            redis_client.hset(redis_key, 'pos_tag', updated_pos_tags)
                    else:
                        # Use the rest of the columns as fields under the generated key
                        data = {field: value for field, value in row.items()}
                        redis_client.hset(redis_key, mapping=data)

                except Exception as e:
                    print(f"Error processing word '{word}': {e}")

    @staticmethod
    def clean_up(csv_file):
        redis_client = RedisManager.redis_client
        
        with open(csv_file, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t')
            for row in reader:
                word = row[reader.fieldnames[0]]  
                word_key = f"word_to_key:{word}"
                
                if redis_client.exists(word_key):
                    redis_client.delete(word_key)

        if redis_client.exists("key_sequence"):
            redis_client.delete("key_sequence")