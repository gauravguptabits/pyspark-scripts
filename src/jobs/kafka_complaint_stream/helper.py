
import nltk
from nltk.corpus import wordnet as wn
import re
import ftfy
# import string
from tabulate import tabulate
import demoji
# import emoji
import contractions


# stop = stopwords.words('english')
# emailDetector = scrubadub.Scrubber(detector_list=[scrubadub.detectors.EmailDetector])
# wnl = WordNetLemmatizer()

label_codes = {'No': 0, 'Yes': 1}
t_handle_regex = r'(^|[^@\w])@(\w{1,15})\b'
t_hashtag_regex = r"#(\w+)"
t_url_regex = r"https?://\S+|www\.\S+"
t_markup_regex = r"<(\"[^\"]*\"|'[^']*'|[^'\">])*>"
t_handle_placeholder = ' {{HANDLE}}'
t_hashtag_placeholder = ' {{HASHTAG}}'
t_url_placeholder = '{{URL}}'
t_markup_placeholder = '{{MARKUP}}'
emoji_placeholder = '{{EMOJI}}'
# domain specific stopwords.


def penn_to_wn(tag):
    def is_noun(tag):
        return tag in ['NN', 'NNS', 'NNP', 'NNPS']

    def is_verb(tag):
        return tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']

    def is_adverb(tag):
        return tag in ['RB', 'RBR', 'RBS']

    def is_adjective(tag):
        return tag in ['JJ', 'JJR', 'JJS']

    # Pos tags to wn tags
    if is_adjective(tag):
        return wn.ADJ
    elif is_noun(tag):
        return wn.NOUN
    elif is_adverb(tag):
        return wn.ADV
    elif is_verb(tag):
        return wn.VERB
    return None


def to_lower_case(text):
    return text.lower()


def fix_unicode(text):
    return ftfy.fix_text(text)


def replace_email(text, emailDetector):
    return emailDetector.clean(text)


def remove_stop_words(text, stop):
    return ' '.join([word for word in text.split() if word not in (stop)])


def convert_emoji_to_text(text):
    return text


def replace_user_name(text):
    return re.sub(t_handle_regex, t_handle_placeholder, text)


def replace_hashtags(text):
    return re.sub(t_hashtag_regex, t_hashtag_placeholder, text)


def replace_url(text):
    return re.sub(t_url_regex, t_url_placeholder, text)


def replace_markup(text):
    return re.sub(t_markup_regex, t_markup_placeholder, text)


def remove_punctuations(text):
    return re.sub(r'[^\w\s]', '', text)


def replace_emoji_with_code(text):
    demoji.replace(text, repl=emoji_placeholder)
    return demoji.replace_with_desc(text)


def get_stats(step, df):
    corpus = " ".join(list(df['text']))
    total_words = len(corpus.split(' '))
    unique_words = len(set(corpus.split(' ')))
    return [step, total_words, unique_words]


def lemmatize(text, lemmatizer):
    default_wn_tag = 'n'
    tokens = text.split(' ')
    pos_tags = nltk.pos_tag(tokens)
    wn_tags = [penn_to_wn(tag) for (w, tag) in pos_tags]
    # print(list(zip(pos_tags, wn_tags)))
    lemmas = [lemmatizer.lemmatize(token, tag or default_wn_tag)
              for (token, tag) in list(zip(tokens, wn_tags))]
    return ' '.join(lemmas)


def fix_contractions(text):
    return contractions.fix(text, slang=False)


def trim_excessive_space(text):
    return " ".join(text.split())


"""
Preprocesses the data.
"""


def process_data(df, lemmatizer, stopwords, emailDetector, **kwargs):
    stats = [['Step', 'Total words', 'Unique words']]
    stats.append(get_stats('Start', df))
    df = df.replace(label_codes)
    df['orig_text'] = df['text']

    if kwargs.get('handle_retweet'):
        df = df[~df['text'].str.startswith('RT')]
        stats.append(get_stats('Remove Retweet', df))
    else:
        stats.append(['Remove Retweet', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_case'):
        df['text'] = df['text'].apply(lambda text: text.lower())
        stats.append(get_stats('Lower', df))
    else:
        stats.append(['Lower', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_contractions'):
        df['text'] = df['text'].apply(lambda text: fix_contractions(text))
        stats.append(get_stats('Remove Retweet', df))
    else:
        stats.append(['Remove Retweet', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_lemmatization'):
        df['text'] = df['text'].apply(lambda text: lemmatize(text, lemmatizer))
        stats.append(get_stats('Lemmatize', df))
    else:
        stats.append(['Lemmatize', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_unicode'):
        df['text'] = df['text'].apply(fix_unicode)
        stats.append(get_stats('Unicode Fix', df))
    else:
        stats.append(['Unicode Fix', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_emoji'):
        df['text'] = df['text'].apply(replace_emoji_with_code)
        stats.append(get_stats('Replace emoji', df))
    else:
        stats.append(['Replace emoji', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_stopwords'):
        df['text'] = df['text'].apply(
            lambda text: remove_stop_words(text, stopwords))
        stats.append(get_stats('Stop words', df))
    else:
        stats.append(['Stop words', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_email'):
        df['text'] = df['text'].apply(
            lambda text: replace_email(text, emailDetector))
        stats.append(get_stats('Email Replace', df))
    else:
        stats.append(['Email Replace', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_username'):
        df['text'] = df['text'].apply(replace_user_name)
        stats.append(get_stats('UserName replace', df))
    else:
        stats.append(['UserName replace', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_hashtags'):
        df['text'] = df['text'].apply(replace_hashtags)
        stats.append(get_stats('HashTags Replace', df))
    else:
        stats.append(['HashTags Replace', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_url'):
        df['text'] = df['text'].apply(replace_url)
        stats.append(get_stats('URL Replace', df))
    else:
        stats.append(['URL Replace', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_markup'):
        df['text'] = df['text'].apply(replace_markup)
        stats.append(get_stats('MARKUP Replace', df))
    else:
        stats.append(['MARKUP Replace', 'xxxxxx', 'xxxxxx'])

    if kwargs.get('handle_punctuation'):
        df['text'] = df['text'].apply(remove_punctuations)
        stats.append(get_stats('Remove punctuation', df))
    else:
        stats.append(['Remove punctuation', 'xxxxxx', 'xxxxxx'])

    df['text'] = df['text'].apply(lambda t: trim_excessive_space(t))
    print(tabulate(stats)) if kwargs.get('print_stats', False) else None
    return df
