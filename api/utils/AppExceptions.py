from http import HTTPStatus

from flask import jsonify


class EmptyDbException(Exception):
    """Exception raised when an API returns an empty.

        Attributes:
            message -- explanation of the error
            error_code -- code of the error
    """

    def __init__(self, message):
        super().__init__(message)
        self.message = message
        self.error_code = HTTPStatus.NO_CONTENT


class NotFoundException(Exception):
    """Exception raised when an API returns an empty.

        Attributes:
            message -- explanation of the error
            error_code -- code of the error
    """

    def __init__(self, message):
        super().__init__(message)
        self.message = message
        self.error_code = HTTPStatus.NOT_FOUND


class NotAuthorized(Exception):
    """Exception raised when user is not authorized to.

        Attributes:
            message -- explanation of the error
            error_code -- code of the error
    """

    def __init__(self, message):
        super().__init__(message)
        self.message = message
        self.error_code = HTTPStatus.UNAUTHORIZED


def handle_maria_db_exception(ex):
    message = str(ex)
    print(message)
    if 'foreign key constraint fails' in str(ex) and 'FOREIGN KEY (`group`)' in str(ex):
        message = 'The group does not exist'
    elif 'foreign key constraint fails' in str(ex) and 'FOREIGN KEY (`class`)' in str(ex):
        message = 'The class does not exist'
    elif ('Duplicate entry' and 'uq_topic_name') in str(ex):
        message = 'Topic title already exists in the same group'
    elif ('Duplicate entry' and 'uq_group_name') in str(ex):
        message = 'Group already exists in the same class'
    return jsonify({'message': message, 'success': False})
