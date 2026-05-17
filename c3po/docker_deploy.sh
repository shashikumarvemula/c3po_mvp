#!/usr/bin/env bash
#aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 852275627372.dkr.ecr.us-west-2.amazonaws.com/ml_annotations_setuserv/chatbot_gpt_interface:latest
docker-compose pull
# rm -rf .setuserv
# VAR1="$1"
# PROD="prod"
# DEV="dev"
# QA="qa"
# echo $VAR1
# if [ "$1" == "$PROD" ]; then
#    cp -rf .setuserv_prod .setuserv
# elif [ "$1" == "$QA" ]; then
#    cp -rf .setuserv_qa .setuserv
# elif [ "$1" == "$DEV" ]; then
#    cp -rf .setuserv_dev .setuserv
# else
#    echo "One argument is required and it has to be prod, qa, or dev"
# fi
# cp -rf .setuserv_qa .setuserv
docker-compose up -d
