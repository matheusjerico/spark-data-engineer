#!/bin/bash

mkdir -p dataset
wget https://storage.googleapis.com/luizalabs-hiring-test/wordcount.txt
mv wordcount.txt dataset/wordcount.txt
wget https://storage.googleapis.com/luizalabs-hiring-test/clientes_pedidos.csv
mv clientes_pedidos.csv dataset/clientes_pedidos.csv
ls dataset/*
