import traceback

import mariadb

from api.database.db import get_connection
from api.models.GroupModel import row_to_group
from api.models.TopicModel import Topic
from api.models.TopicModel import row_to_topic, Topic
from api.utils.AppExceptions import NotFoundException, EmptyDbException
from api.utils.Logger import Logger
from api.utils.QueryParameters import QueryParameters


class TopicService:

    @classmethod
    def get_all_topics(cls, params: QueryParameters) -> list[Topic]:
        try:
            connection_dbgroups = get_connection('dbgroups')
            topics_list = []
            with (connection_dbgroups.cursor()) as cursor_dbtopics:
                query = "select * from topics"
                query = params.add_to_query(query)
                cursor_dbtopics.execute(query)
                result_set = cursor_dbtopics.fetchall()
                if not result_set:
                    raise EmptyDbException("No topics found")
                for row in result_set:
                    topic = row_to_topic(row)
                    topics_list.append(topic)
            connection_dbgroups.close()
            return topics_list
        except mariadb.OperationalError:
            raise
        except EmptyDbException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def get_topic_by_id(cls, topicId: int) -> Topic:
        try:
            connection_dbgroups = get_connection('dbgroups')
            topic = None
            with connection_dbgroups.cursor() as cursor_dbtopics:
                query = "select * from topics where id = '{}'".format(topicId)
                cursor_dbtopics.execute(query)
                row = cursor_dbtopics.fetchone()
                if row is not None:
                    topic = row_to_topic(row)
                else:
                    raise NotFoundException("Topic not found")
            connection_dbgroups.close()
            return topic
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def add_topic(cls, topic: Topic):
        try:
            connection_dbgroups = get_connection('dbgroups')
            with (connection_dbgroups.cursor()) as cursor_dbtopics:
                query = "insert into `topics` set title = '{}', deadline='{}', unit = '{}'".format(
                    topic.title, topic.deadline, topic.unit)
                cursor_dbtopics.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return 'Topic added'
        except mariadb.IntegrityError:
            # Topic already exists
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def delete_topic(cls, topicId: int):
        try:
            # Check if topic exists
            cls.get_topic_by_id(topicId)
            connection_dbgroups = get_connection('dbgroups')
            with (connection_dbgroups.cursor()) as cursor_dbtopics:
                query = "delete from `topics` where id = '{}'".format(topicId)
                cursor_dbtopics.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return f'Topic {topicId} deleted'
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def delete_topic_group(cls, topicId: int, groupId: int):
        try:
            connection_dbgroups = get_connection('dbgroups')
            with ((connection_dbgroups.cursor()) as cursor_dbgroups):
                query = ("delete from relationtopicsgroups "
                         "where topic = '{}' and `group` = '{}'").format(topicId, groupId)
                cursor_dbgroups.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return f'Group {groupId} from Topic {topicId} has been deleted'
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def update_topic(cls, topic: Topic):
        try:
            # Check if topic exists
            cls.get_topic_by_id(topic.topicId)
            connection_dbgroups = get_connection('dbgroups')
            with (connection_dbgroups.cursor()) as cursor_dbtopics:
                query = (
                    "update topics set title = '{}', deadline='{}', unit = '{}' "
                    "where id = '{}'").format(
                    topic.title, topic.deadline, topic.unit, topic.topicId)
                cursor_dbtopics.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return f'Topic {topic.topicId} updated'
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def assign_group(cls, topic_id, group_id):
        try:
            connection_dbgroups = get_connection('dbgroups')
            with connection_dbgroups.cursor() as cursor_dbgroups:
                query = "insert into relationtopicsgroups set topic='{}', `group`='{}'".format(topic_id, group_id)
                cursor_dbgroups.execute(query)
                connection_dbgroups.commit()
            connection_dbgroups.close()
            return 'Group was assigned to topic successfully'
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def get_topic_groups(cls, topic_id):
        try:
            connection_dbgroups = get_connection('dbgroups')
            groups_list = []
            with connection_dbgroups.cursor() as cursor_dbgroups:
                query = ("SELECT id, NAME, DESCRIPTION, class FROM relationtopicsgroups a "
                         "INNER JOIN `groups` b ON a.group = b.id WHERE topic = '{}'").format(topic_id)
                cursor_dbgroups.execute(query)
                result_set = cursor_dbgroups.fetchall()
                if not result_set:
                    raise EmptyDbException("No groups found")
                for row in result_set:
                    group = row_to_group(row)
                    groups_list.append(group)
            connection_dbgroups.close()
            return groups_list
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise

    @classmethod
    def get_topic_remaining_groups(cls, topic_id):
        try:
            connection_dbgroups = get_connection('dbgroups')
            groups_list = []
            with connection_dbgroups.cursor() as cursor_dbgroups:
                query = ("SELECT c.id, c.NAME, c.DESCRIPTION, c.class FROM `groups` c "
                         "LEFT JOIN(SELECT id, NAME, DESCRIPTION, class FROM relationtopicsgroups a "
                         "INNER JOIN `groups` b ON a.group = b.id WHERE topic = '{}')"
                         "d ON c.id = d.id WHERE d.id IS NULL").format(topic_id)
                cursor_dbgroups.execute(query)
                result_set = cursor_dbgroups.fetchall()
                if not result_set:
                    raise EmptyDbException("No groups found")
                for row in result_set:
                    group = row_to_group(row)
                    groups_list.append(group)
            connection_dbgroups.close()
            return groups_list
        except NotFoundException:
            raise
        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            raise