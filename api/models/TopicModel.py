class Topic:

    def __init__(self, topicId, groupId, title, deadline, unit):
        self.topicId = topicId
        self.groupId = groupId
        self.title = title
        self.deadline = deadline
        self.unit = unit

    def to_json(self):
        return {
            'topicId': self.topicId,
            'groupId': self.groupId,
            'title': self.title,
            'deadline': self.deadline,
            'unit': self.unit
        }


def row_to_topic(row):
    return Topic(
        topicId=row[0],
        groupId=row[1],
        title=row[2],
        deadline=row[3],
        unit=row[4]
    )
