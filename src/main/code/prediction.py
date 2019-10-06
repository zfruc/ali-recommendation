import os
import sys
import json
import pandas as pd
import tensorflow as tf
from flink_ml_tensorflow import tensorflow_on_flink_ops as tff_ops
from flink_ml_tensorflow.tensorflow_context import *


def create_feature():
    item_gender = tf.feature_column.categorical_column_with_vocabulary_list(
        "item_gender", ["female", "male"])

    pred_gender = tf.feature_column.categorical_column_with_vocabulary_list(
        "pred_gender", ["female", "male"])

    pred_has_car = tf.feature_column.categorical_column_with_vocabulary_list(
        "pred_has_car", ["yes", "no"])

    pred_has_pet = tf.feature_column.categorical_column_with_vocabulary_list(
        "pred_has_pet", ["yes", "no"])

    pred_has_house = tf.feature_column.categorical_column_with_vocabulary_list(
        "pred_has_house", ["yes", "no"])

    os = tf.feature_column.categorical_column_with_vocabulary_list(
        "os", ["apple", "android"])

    pred_life_stage = tf.feature_column.categorical_column_with_vocabulary_list(
        "pred_life_stage", ["married", "unmarried"])

    pred_career_type = tf.feature_column.categorical_column_with_vocabulary_list(
        "pred_career_type", [
            "R&D personnel", "civilian staff", "Counter person", "professor", "Designer",
            "Financial officer", "judge", "lawyer", "Clerk", "Guard", "editor", "Doctors",
            "nurse", "engineer", "Laboratory staff"
        ])

    pred_education_degree = tf.feature_column.categorical_column_with_vocabulary_list(
        "pred_education_degree", [
            "Postgraduate", "Undergraduate", "University specialties",
            "specialized middle school", "Technical", "High school",
            "junior high school", "primary school", "illiteracy"
        ])

    item_category = tf.feature_column.categorical_column_with_vocabulary_list(
        "item_category", [
            "Women's clothing", "Men's", "Women's shoes", "makeups", "Watch", "Mobile phone",
            "Maternal baby", "toy", "Snack", "Tea wine", "Fresh", "fruit", "Home appliance",
            "Furniture", "Building materials", "car", "Accessories", "Home textile", "medicine",
            "Kitchenware", "Storage"
        ])

    item_material_id = tf.feature_column.categorical_column_with_hash_bucket(
        "item_material_id", hash_bucket_size=20, dtype=tf.int64)

    item__i_shop_id_ctr = tf.feature_column.numeric_column("item__i_shop_id_ctr")
    item__i_category_ctr = tf.feature_column.numeric_column("item__i_category_ctr")
    item__i_brand_id_ctr = tf.feature_column.numeric_column("item__i_brand_id_ctr")

    crossed_columns = [
        tf.feature_column.crossed_column(
            ["item_gender", "pred_gender"], hash_bucket_size=4),
        tf.feature_column.crossed_column(
            ["item_style_id", "item_material_id", "item_category"], hash_bucket_size=100),
        tf.feature_column.crossed_column(
            ["pred_age_level", "item_purch_level", "item_material_id"], hash_bucket_size=100)
    ]

    deep_columns = [
        tf.feature_column.indicator_column(item_gender),
        tf.feature_column.indicator_column(pred_gender),
        tf.feature_column.indicator_column(pred_has_car),
        tf.feature_column.indicator_column(pred_has_pet),
        tf.feature_column.indicator_column(pred_has_house),
        tf.feature_column.indicator_column(os),
        tf.feature_column.indicator_column(pred_life_stage),
        tf.feature_column.indicator_column(pred_career_type),
        tf.feature_column.indicator_column(pred_education_degree),
        tf.feature_column.indicator_column(item_category),
        tf.feature_column.embedding_column(item_material_id, dimension=8),
        item__i_shop_id_ctr,
        item__i_category_ctr,
        item__i_brand_id_ctr,
    ]
    return crossed_columns, deep_columns


def input_iter(context, batch_size):
    dataset = context.flink_stream_dataset()
    dataset = dataset.batch(batch_size)
    iterator = dataset.make_one_shot_iterator()
    return iterator


def predict(json, ckpt_path):
    (crossed_columns, deep_columns) = create_feature()

    df_data = pd.read_json(json, orient='record')
    ids = df_data["item_id"]
    userId = df_data["user_id"][0]

    m = tf.estimator.DNNLinearCombinedClassifier(
        linear_feature_columns=crossed_columns,
        dnn_feature_columns=deep_columns,
        dnn_hidden_units=[100, 50])
    predictions = m.predict(
        input_fn=data_sample(df_data),
        predict_keys=None,
        hooks=None,
        checkpoint_path=ckpt_path,
        yield_single_examples=True)

    res = str(userId)+","

    for id, pred_dict in zip(ids, predictions):
        if pred_dict:
            res += (str(id) + "-" + str(pred_dict['probabilities'][1]) + "|")

    return res


def data_sample(df_data):
    return tf.estimator.inputs.pandas_input_fn(
        x=df_data,
        y=None,
        batch_size=1,
        num_epochs=None,
        shuffle=False,
        num_threads=1)


def map_func(context):
    print(tf.__version__)
    props = context.properties
    ckpt = props.get('ckpt')
    if "ckpt_path_prefix" in props:
        ckpt_path = props.get("ckpt_path_prefix") + "/" + ckpt
    else:
        path = os.path.dirname(os.path.abspath(__file__))
        ckpt_path = path + "/" + ckpt

    tf_context = TFContext(context)
    session = tf.Session()
    iter = input_iter(tf_context, 1)
    next_batch = iter.get_next()
    fw = tff_ops.FlinkTFRecordWriter(address=context.to_java())

    while True:
        value = session.run([next_batch[0]])
        json = "[" + value[0] + "]"
        res = predict(json, ckpt_path)
        w = fw.write([res])
        session.run(w)


if __name__ == "__main__":
    predict()
