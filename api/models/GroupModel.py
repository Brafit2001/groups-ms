
class Group:

    def __init__(self, groupId, name, description, classId):
        self.groupId = groupId
        self.name = name
        self.description = description
        self.classId = classId

    def to_json(self):
        return {
            'groupId': self.groupId,
            'name': self.name,
            'description': self.description,
            'classId': self.classId
        }
