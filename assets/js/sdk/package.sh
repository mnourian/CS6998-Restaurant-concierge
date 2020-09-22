
pip3.9 install --target ./package requests-aws4auth opensearch-py

cd package
zip -r ../deployment.zip .

cd ..
zip deployment.zip lambda_function.py
