# eval $(aws ecr get-login --no-include-email)

docker build . -t agents_chatbots_image_cms
# aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 852275627372.dkr.ecr.us-west-2.amazonaws.com/ml_annotations_setuserv/auto_chatbots_interface:latest

# current_epoch=$(date +%s)
# docker tag hybrid_chatbots_image 852275627372.dkr.ecr.us-west-2.amazonaws.com/ml_annotations_setuserv/auto_chatbots_interface:latest
# docker tag hybrid_chatbots_image 852275627372.dkr.ecr.us-west-2.amazonaws.com/ml_annotations_setuserv/auto_chatbots_interface:${current_epoch}
#
# docker push 852275627372.dkr.ecr.us-west-2.amazonaws.com/ml_annotations_setuserv/auto_chatbots_interface:latest
# docker push 852275627372.dkr.ecr.us-west-2.amazonaws.com/ml_annotations_setuserv/auto_chatbots_interface:${current_epoch}
