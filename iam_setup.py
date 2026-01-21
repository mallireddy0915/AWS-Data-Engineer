import boto3
import json
from botocore.exceptions import ClientError

# Initialize the IAM client
iam = boto3.client('iam')

def create_iam_structure():
    try:
        # 1. Create a User Group
        group_name = "DataEngineeringGroup"
        iam.create_group(GroupName=group_name)
        print(f"Group '{group_name}' created.")

        # 2. Create a User
        user_name = "PythonDataBot"
        iam.create_user(UserName=user_name)
        print(f"User '{user_name}' created.")

        # 3. Add User to Group
        iam.add_user_to_group(GroupName=group_name, UserName=user_name)
        print(f"User added to group.")

        # 4. Create a Role (Trust Policy is required)
        role_name = "S3ReadOnlyRole_Boto3"
        
        # This JSON defines WHO can assume the role (e.g., EC2)
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        print(f"Role '{role_name}' created.")

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("Resource already exists. Skipping creation.")
        else:
            print(f"Unexpected error: {e}")

if __name__ == "__main__":
    create_iam_structure()