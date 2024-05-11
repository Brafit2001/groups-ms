import traceback
from http import HTTPStatus

import mariadb
from flask import Blueprint, jsonify, request
import datetime
from api.models.PermissionModel import PermissionName, PermissionType
from api.models.TopicModel import Topic
from api.services.TopicService import TopicService
from api.utils.AppExceptions import EmptyDbException, NotFoundException, handle_maria_db_exception
from api.utils.Logger import Logger
from api.utils.QueryParameters import QueryParameters
from api.utils.Security import Security

topics = Blueprint('topics_blueprint', __name__)


@topics.route('/', methods=['GET'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.READ)])
def get_all_topics():
    try:
        params = QueryParameters(request)
        topics_list = TopicService.get_all_topics(params)
        response_topics = []
        for topic in topics_list:
            response_topics.append(topic.to_json())
        response = jsonify({'success': True, 'data': response_topics})
        return response, HTTPStatus.OK
    except mariadb.OperationalError as ex:
        response = jsonify({'success': False, 'message': str(ex)})
        return response, HTTPStatus.SERVICE_UNAVAILABLE
    except EmptyDbException as ex:
        response = jsonify({'success': False, 'message': ex.message})
        return response, ex.error_code
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/<topic_id>', methods=['GET'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.READ)])
def get_topic_by_id(topic_id: int):
    try:
        topic_id = int(topic_id)
        topic = TopicService.get_topic_by_id(topic_id)
        response = jsonify({'success': True, 'data': topic.to_json()})
        return response, HTTPStatus.OK
    except NotFoundException as ex:
        response = jsonify({'message': ex.message, 'success': False})
        return response, ex.error_code
    except ValueError:
        return jsonify({'message': "Topic id must be an integer", 'success': False})
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/<topic_id>/groups', methods=['GET'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.READ)])
def get_topic_groups(topic_id: int):
    try:
        topic_id = int(topic_id)
        groups_list = TopicService.get_topic_groups(topic_id)
        response_groups = []
        for group in groups_list:
            response_groups.append(group.to_json())
        response = jsonify({'success': True, 'data': response_groups})
        return response, HTTPStatus.OK
    except EmptyDbException as ex:
        response = jsonify({'success': False, 'message': ex.message})
        return response, ex.error_code
    except NotFoundException as ex:
        response = jsonify({'message': ex.message, 'success': False})
        return response, ex.error_code
    except ValueError:
        return jsonify({'message': "Topic id must be an integer", 'success': False})
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/<topic_id>/rubrics', methods=['GET'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.READ)])
def get_topic_rubrics(topic_id: int):
    try:
        topic_id = int(topic_id)
        rubrics_list = TopicService.get_topic_rubrics(topic_id)
        response_rubrics = []
        for rubric in rubrics_list:
            response_rubrics.append(rubric.to_json())
        response = jsonify({'success': True, 'data': response_rubrics})
        return response, HTTPStatus.OK
    except EmptyDbException as ex:
        response = jsonify({'success': False, 'message': ex.message})
        return response, ex.error_code
    except NotFoundException as ex:
        response = jsonify({'message': ex.message, 'success': False})
        return response, ex.error_code
    except ValueError:
        return jsonify({'message': "Topic id must be an integer", 'success': False})
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR



@topics.route('/<topic_id>/groups-remaining', methods=['GET'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.READ)])
def get_topic_remaining_groups(topic_id: int):
    try:
        topic_id = int(topic_id)
        groups_list = TopicService.get_topic_remaining_groups(topic_id)
        response_groups = []
        for group in groups_list:
            response_groups.append(group.to_json())
        response = jsonify({'success': True, 'data': response_groups})
        return response, HTTPStatus.OK
    except EmptyDbException as ex:
        response = jsonify({'success': False, 'message': ex.message})
        return response, ex.error_code
    except NotFoundException as ex:
        response = jsonify({'message': ex.message, 'success': False})
        return response, ex.error_code
    except ValueError:
        return jsonify({'message': "Topic id must be an integer", 'success': False})
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/', methods=['POST'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.WRITE)])
def add_topic():
    try:
        # Guardamos la fecha en formato datetime
        deadline = datetime.datetime.fromisoformat(request.json["deadline"])
        # Convertimos a utc y guardamos en formato Y-m-d H:M:S
        deadline_utc = deadline.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        _topic = Topic(topicId=0, title=request.json["title"],
                       deadline=deadline_utc, unit=request.json["unit"])

        TopicService.add_topic(_topic)
        response = jsonify({'message': 'Topic created successfully', 'success': True})
        return response, HTTPStatus.OK
    except KeyError:
        response = jsonify({'message': 'Bad body format', 'success': False})
        return response, HTTPStatus.BAD_REQUEST
    except mariadb.IntegrityError as ex:
        response = handle_maria_db_exception(ex)
        return response, HTTPStatus.BAD_REQUEST
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/<topic_id>', methods=['DELETE'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.WRITE)])
def delete_topic(topic_id):
    try:
        response_message = TopicService.delete_topic(topic_id)
        response = jsonify({'message': response_message, 'success': True})
        return response, HTTPStatus.OK
    except NotFoundException as ex:
        response = jsonify({'success': False, 'message': ex.message})
        return response, ex.error_code
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/<topic_id>/groups/<group_id>', methods=['DELETE'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.GROUPS_MANAGER, PermissionType.WRITE)])
def delete_topic_group(*args, **kwargs):
    try:
        topic_id = int(kwargs["topic_id"])
        group_id = int(kwargs["group_id"])
        response_message = TopicService.delete_topic_group(topicId=topic_id, groupId=group_id)
        response = jsonify({'message': response_message, 'success': True})
        return response, HTTPStatus.OK
    except NotFoundException as ex:
        response = jsonify({'message': ex.message, 'success': False})
        return response, ex.error_code
    except ValueError:
        return jsonify({'message': "Group and User id must be an integer", 'success': False})
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/<topic_id>', methods=['PUT'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.WRITE)])
def edit_topic(topic_id):
    try:
        _topic = Topic(topicId=topic_id, title=request.json["title"],
                       deadline=request.json["deadline"], unit=request.json["unit"])
        response_message = TopicService.update_topic(_topic)
        response = jsonify({'message': response_message, 'success': True})
        return response, HTTPStatus.OK
    except KeyError:
        response = jsonify({'message': 'Bad body format', 'success': False})
        return response, HTTPStatus.BAD_REQUEST
    except mariadb.IntegrityError as ex:
        response = handle_maria_db_exception(ex)
        return response, HTTPStatus.BAD_REQUEST
    except NotFoundException as ex:
        response = jsonify({'success': False, 'message': ex.message})
        return response, ex.error_code
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@topics.route('/assign-group-to-topic', methods=['POST'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.TOPICS_MANAGER, PermissionType.WRITE)])
def assign_group_to_topic():
    try:
        topic_id = request.json.get('topic')
        group_id = request.json.get('group')
        response_message = TopicService.assign_group(topic_id, group_id)
        response = jsonify({'message': response_message, 'success': True})
        return response, HTTPStatus.OK
    except mariadb.IntegrityError as ex:
        response = handle_maria_db_exception(ex)
        return response, HTTPStatus.BAD_REQUEST
    except KeyError:
        response = jsonify({'message': 'Bad body format', 'success': False})
        return response, HTTPStatus.BAD_REQUEST
    except NotFoundException as ex:
        response = jsonify({'success': False, 'message': ex.message})
        return response, ex.error_code
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR
