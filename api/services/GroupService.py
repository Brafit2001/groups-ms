import traceback

import mariadb

from api.database.db import get_connection
from api.models.GroupModel import Group
from api.utils.AppExceptions import NotFoundException, EmptyDbException
from api.utils.Logger import Logger


class GroupService:

    @classmethod
    def get_all_groups(cls) -> list[Group]:
        try:
            connection_dbgroups = get_connection('dbgroups')
            groups_list = []
            with (connection_dbgroups.cursor()) as cursor_dbgroups:
                query = "select * from groups"
                cursor_dbgroups.execute(query)
                result_set = cursor_dbgroups.fetchall()
                if not result_set:
                    raise EmptyDbException("No groups found")
                for row in result_set:
                    group = row_to_group(row)
                    groups_list.append(group)
            connection_dbgroups.close()
            return groups_list
        except mariadb.OperationalError:
            raise
        except EmptyDbException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def get_group_by_id(cls, groupId: int) -> Group:
        try:
            connection_dbgroups = get_connection('dbgroups')
            group = None
            with connection_dbgroups.cursor() as cursor_dbgroups:
                query = "select * from groups where id = '{}'".format(groupId)
                cursor_dbgroups.execute(query)
                row = cursor_dbgroups.fetchone()
                if row is not None:
                    group = row_to_group(row)
                else:
                    raise NotFoundException("Group not found")
            connection_dbgroups.close()
            return group
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def add_group(cls, group: Group):
        try:
            connection_dbgroups = get_connection('dbgroups')
            with (connection_dbgroups.cursor()) as cursor_dbgroups:
                query = "insert into `groups` set name = '{}', description = '{}', class='{}'".format(
                    group.name, group.description, group.classId)
                cursor_dbgroups.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return 'Group added'
        except mariadb.IntegrityError:
            # Group already exists
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def delete_group(cls, groupId: int):
        try:
            # Check if group exists
            cls.get_group_by_id(groupId)
            connection_dbgroups = get_connection('dbgroups')
            with (connection_dbgroups.cursor()) as cursor_dbgroups:
                query = "delete from `groups` where id = '{}'".format(groupId)
                cursor_dbgroups.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return f'Group {groupId} deleted'
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def update_group(cls, group: Group):
        try:
            # Check if group exists
            cls.get_group_by_id(group.groupId)
            connection_dbgroups = get_connection('dbgroups')
            with (connection_dbgroups.cursor()) as cursor_dbgroups:
                query = (
                    "update `groups` set name = '{}', description = '{}', class='{}' "
                    "where id = '{}'").format(
                    group.name, group.description, group.classId, group.groupId)
                cursor_dbgroups.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return f'Group {group.groupId} updated'
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def assign_group(cls, userId: int, groupId: int):
        try:
            connection_dbgroups = get_connection('dbgroups')
            with connection_dbgroups.cursor() as cursor_dbgroups:
                query = "insert into relationusersgroups set user='{}', `group`='{}'".format(userId, groupId)
                cursor_dbgroups.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return 'User was assigned to group successfully'
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise


def row_to_group(row):
    return Group(
        groupId=row[0],
        name=row[1],
        description=row[2],
        classId=row[3]
    )
