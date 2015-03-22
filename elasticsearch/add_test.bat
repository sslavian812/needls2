for /r %%i in (test\*) do curl -XPOST "http://localhost:9200/users/user/" -d @%%i

pause