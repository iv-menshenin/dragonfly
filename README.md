# dragonfly
Database design, deployment and practical use

## How it all started
I have to constantly design databases, but that is not all.
I have to create them on the server, I have to support and develop them.
Often necessary SQL queries to change metadata, search where I should change this in our services.
And sometimes, to find in the service code all the places where the changed object is used, I have to press Ctrl + F.
Of course, all this makes me sad.
And the most annoying is if I did not make a mistake in the design, but made a mistake in not finding all the places in the service code that need to be changed.
How to connect them so that the change in metadata in the database is explicit for the service code?
So this project appeared.

## How this app helps me
The main idea is that in fact explicit changes occur only in the design, all other changes - changes to the database code, changes to the services code, some other dependencies - all this should change as a dependence on design.
Therefore, a good option is automatic code generation.
Now this program allows me to store the database design in yaml format in separate files, generate a migration sql-code from this file for the database and generate API code for working with database structures in GO language.

Thus, I make explicit design changes, start code generation, and then the project build, and I see that some services could not be compiled due to the fact that their code were broken.
I just fix all the places where the error occurred and I’m sure I didn’t miss anything, because auto-generation cannot forget a single piece of code where the changed object is used.
This happens if your design has undergone significant changes - the name of the field or table has changed, but if the changes are not significant, we just get a workable code that already takes into account new changes.
This happens, for example, when I add a column to the table that should be filled automatically.
In this case, an API is generated in which the signatures of the functions or data structure do not change, but the implementation itself changes.

This greatly improves the development process.
This application can be especially useful for startups, when the time taken to create an database API can be very expensive.

## How to use it
This application is provided as a library in the GO language.
You can see how to use its API in the app directory.
The application comes with several files that must be copied to the directory with the generated code, these are additional functions for implementing the API database.

## It would be nice to read more
I apologize, in fact the application is still in development.
It can already be used, but not all SQL migration code is generated well, something else needs to be fixed.
But the GO code is pretty good.

However, I want to assure you that the work on this project is ongoing - I actively use it in another project that is constantly evolving and really needs automatic code generation.