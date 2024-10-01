import pandas as pd
from deep_translator import GoogleTranslator
import re
import time

# Снятие ограничения на отображение колонок в консоли
pd.set_option('display.max_columns', None)

# Загрузка данных
data = pd.read_csv('/Users/alyona/Downloads/Resume.csv')

# Инициализация переводчика
translator = GoogleTranslator(source='en', target='ru')

# Функция для разбиения текста на предложения
def split_text_to_sentences(text):
    sentences = re.split(r'(?<=[.!?]) +', text)
    return sentences

# Функция для перевода текста
def translate_text(text):
    if isinstance(text, str):
        sentences = split_text_to_sentences(text)
        translated_sentences = []
        for sentence in sentences:
            attempts = 3
            while attempts > 0:
                try:
                    translated_sentence = translator.translate(sentence)
                    if translated_sentence:
                        translated_sentences.append(translated_sentence)
                    else:
                        translated_sentences.append(sentence)  # В случае ошибки добавляем оригинальное предложение
                    break
                except Exception as e:
                    attempts -= 1
                    time.sleep(1)  # Ждем 1 секунду перед повторной попыткой
                    if attempts == 0:
                        translated_sentences.append(sentence)  # В случае ошибки добавляем оригинальное предложение
        return ' '.join(translated_sentences)
    return text

# Обработка данных по частям и сохранение промежуточных результатов
batch_size = 10  # Размер пакета
total_batches = len(data) // batch_size + (1 if len(data) % batch_size != 0 else 0)

for batch_number in range(total_batches):
    start = batch_number * batch_size
    end = start + batch_size
    data_batch = data.iloc[start:end]
    data_batch['Resume_str'] = data_batch['Resume_str'].apply(translate_text)
    data_batch['Category'] = data_batch['Category'].apply(translate_text)
    if start == 0:
        data_batch.to_csv('Resume_translated_partial.csv', index=False)
    else:
        data_batch.to_csv('Resume_translated_partial.csv', mode='a', header=False, index=False)
    time.sleep(5)  # Задержка в 5 секунд между пакетами

# Считываем все части в один DataFrame и сохраняем финальный результат
translated_data = pd.read_csv('Resume_translated_partial.csv')
translated_data.to_csv('Resume_translated_final.csv', index=False)
