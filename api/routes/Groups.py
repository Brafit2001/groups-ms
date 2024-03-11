import traceback
from http import HTTPStatus

import mariadb
from flask import Blueprint, jsonify, request

from api.models.GroupModel import Group
from api.models.PermissionModel import PermissionName, PermissionType
from api.services.GroupService import GroupService
from api.utils.AppExceptions import EmptyDbException, NotFoundException, handle_maria_db_exception
from api.utils.Logger import Logger
from api.utils.Security import Security

groups = Blueprint('groups_blueprint', __name__)


@groups.route('/', methods=['GET'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.GROUPS_MANAGER, PermissionType.READ)])
def get_all_groups():
    try:
        groups_list = GroupService.get_all_groups()
        response_groups = []
        for group in groups_list:
            response_groups.append(group.to_json())
        response = jsonify({'success': True, 'data': response_groups})
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


@groups.route('/<group_id>', methods=['GET'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.GROUPS_MANAGER, PermissionType.READ)])
def get_group_by_id(group_id: int):
    try:
        group_id = int(group_id)
        group = GroupService.get_group_by_id(group_id)
        response = jsonify({'success': True, 'data': group.to_json()})
        return response, HTTPStatus.OK
    except NotFoundException as ex:
        response = jsonify({'message': ex.message, 'success': False})
        return response, ex.error_code
    except ValueError:
        return jsonify({'message': "Group id must be an integer", 'success': False})
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        response = jsonify({'message': str(ex), 'success': False})
        return response, HTTPStatus.INTERNAL_SERVER_ERROR


@groups.route('/', methods=['POST'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.GROUPS_MANAGER, PermissionType.WRITE)])
def add_group():
    try:

        _group = Group(groupId=0,
                       name=request.json["name"], description=request.json["description"],classId=request.json["class"])
        GroupService.add_group(_group)
        response = jsonify({'message': 'Group created successfully', 'success': True})
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


@groups.route('/<group_id>', methods=['DELETE'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.GROUPS_MANAGER, PermissionType.WRITE)])
def delete_group(group_id):
    try:
        response_message = GroupService.delete_group(group_id)
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


@groups.route('/<group_id>', methods=['PUT'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.GROUPS_MANAGER, PermissionType.WRITE)])
def edit_group(group_id):
    try:
        _group = Group(groupId=group_id,
                       name=request.json["name"], description=request.json["description"],classId=request.json["class"])
        response_message = GroupService.update_group(_group)
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


@groups.route('/assign-user-to-group', methods=['POST'])
@Security.authenticate
@Security.authorize(permissions_required=[(PermissionName.GROUPS_MANAGER, PermissionType.WRITE)])
def assign_group():
    try:
        user_id = int(request.json["user"])
        group_id = int(request.json["group"])
        response_message = GroupService.assign_group(user_id, group_id)
        response = jsonify({'message': response_message, 'success': True})
        return response, HTTPStatus.OK
    except mariadb.IntegrityError as ex:
        message = str(ex)
        if "Duplicate entry" in message:
            message = "User is already assigned to the group"
        elif "foreign key constraint fails" in message and "FOREIGN KEY (`user`)" in message:
            message = "User does not exist"
        elif "foreign key constraint fails" in message and "FOREIGN KEY (`group`)" in message:
            message = "Group does not exist"
        response = jsonify({'message': message, 'success': False})
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

