# Databricks notebook source
print(";el")

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create a dataframe").getOrCreate()

data = [(1, "Suresh"), (2, "ravi")]
columns = ["id", "Name"]

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

def my_function():
    print("hello")

# COMMAND ----------

def add(a, b):
    return  a+b


# COMMAND ----------

result = add(5, 5)

# COMMAND ----------

me = add(10, 23)
print(me)

# COMMAND ----------

def suresh_add(a, b, c, d):
    
    return a+b-c+d



# COMMAND ----------

result = suresh_add(10,11, 23, 24)

# COMMAND ----------

print(result)

# COMMAND ----------

############################ ------------------------------------------------------- Level 1: Basic Function
# A simple function to print "Hello, world!"
def my_function():
    print("Hello, world!")  # This line prints the message "Hello, world!"

# Call the basic function
my_function()  # This calls the function and prints "Hello, world!"

############################ ------------------------------------------------------- Level 2: Function with Parameters
# A function that takes a parameter 'name' and greets the user
def suresh(name):
    print(f"Hello, {name}!")  # This prints a greeting using the 'name' argument

# Call the 'suresh' function with the argument "ravi"
suresh("ravi")  # This will print: "Hello, ravi!"

############################ ------------------------------------------------------- Level 3: Return Values
# A function that adds two numbers and returns the result
def suresh_add(a, b):
    """ 
    This function adds two numbers.
    Parameters:
    a (int/float): The first number to add.
    b (int/float): The second number to add.

    Returns:
    int/float: The sum of a and b.
    """
    return a + b  # The sum of 'a' and 'b' is returned

# Call the 'suresh_add' function with arguments 10 and 20
result = suresh_add(10, 20)  # The function returns 30, which is stored in 'result'
print(f"The sum is: {result}")  # This prints: "The sum is: 30"

############################ ------------------------------------------------------- Level 4: Default Parameters
# A function with a default parameter value
def suresh_name(name="Guest"):
    print(f"Hello, {name}!")  # This prints a greeting with the 'name' argument

# Call the function without an argument (uses default 'Guest')
suresh_name()  # This will print: "Hello, Guest!"

# Call the function with a custom argument "John"
suresh_name("John")  # This will print: "Hello, John!"

############################ ------------------------------------------------------- Level 5: Docstrings
# A function that adds two numbers with a docstring
def add(a, b):
    """
    This function adds two numbers
    This is a docstring explaining what the function does.
    """
    return a + b  # This adds 'a' and 'b' and returns the result

############################  ------------------------------------------------------- Level 6: Variable Scope
# A demonstration of variable scope (local vs global)
global_var = 10  # This is a global variable

def some_function():
    local_var = 5  # This is a local variable inside the function
    print(global_var + local_var)  # The function prints the sum of global and local variables

some_function()  # This will print: 15 (10 from global_var and 5 from local_var)

############################ ------------------------------------------------------- Level 7: Recursion
# A recursive function to calculate the factorial of a number
def factorial(n):
    if n == 0:  # Base case: factorial of 0 is 1
        return 1
    else:  # Recursive case: multiply the number by the factorial of (n-1)
        return n * factorial(n - 1)

# Call the factorial function with 5
result = factorial(5)  # The factorial of 5 is calculated as 5 * 4 * 3 * 2 * 1 = 120
print(f"Factorial of 5 is: {result}")  # This will print: "Factorial of 5 is: 120"

############################ ------------------------------------------------------- Level 8: Lambda Functions
# A lambda function to double a number
double = lambda x: x * 2  # This defines a function that multiplies 'x' by 2

# Call the lambda function with 3
result = double(3)  # The result is 6 (3 * 2)
print(f"Double of 3 is: {result}")  # This will print: "Double of 3 is: 6"

############################ ------------------------------------------------------- Level 9: Function Decorators
# A decorator function that adds extra functionality before and after a function is called
def my_decorator(func):
    def wrapper():
        print("Something is happening before the function is called.")  # This runs before the function
        func()  # Call the original function
        print("Something is happening after the function is called.")  # This runs after the function
    return wrapper  # Return the wrapper function

# Apply the decorator to the function 'say_hello'
@my_decorator
def say_hello():
    print("Hello!")  # This is the original function's behavior

# Call the decorated function
say_hello()  # This will print:
# "Something is happening before the function is called."
# "Hello!"
# "Something is happening after the function is called."

############################ ------------------------------------------------------- Advanced Functions: Level 10 - Higher-Order Functions
# A higher-order function that takes a function and a number, then applies the function to the number
def apply_function(f, x):
    return f(x)  # Call the function 'f' with the argument 'x' and return the result

# A function to square a number
def square(n):
    return n * n  # This returns the square of 'n'

# Apply the 'apply_function' with the 'square' function and the number 4
result = apply_function(square, 4)  # The result is 16 (4 * 4)
print(f"Square of 4 is: {result}")  # This will print: "Square of 4 is: 16"

############################ -------------------------------------------------------Advanced Functions: Level 11 - Nested Functions
# A function that contains another function inside it
def outer_function(x):
    # This is the inner function
    def inner_function(y):
        return y + 10  # This adds 10 to 'y'
    
    return inner_function(x)  # Call the inner function with 'x' as the argument

# Call the outer function with 5
result = outer_function(5)  # The inner function adds 10 to 5, so the result is 15
print(f"Result of nested function is: {result}")  # This will print: "Result of nested function is: 15"

############################ ------------------------------------------------------- Advanced Functions: Level 12 - Partial Functions
# Partial functions allow us to fix some arguments of a function
from functools import partial

# A function that multiplies two numbers
def multiply(a, b):
    return a * b  # This multiplies 'a' and 'b'

# Create a partial function that always multiplies by 2
double_multiply = partial(multiply, 2)  # Now, 'double_multiply' takes only one argument

# Call the partial function with the argument 5
result = double_multiply(5)  # This multiplies 2 by 5, so the result is 10
print(f"Multiplying 2 by 5 gives: {result}")  # This will print: "Multiplying 2 by 5 gives: 10"

############################  ------------------------------------------------------- Advanced Functions: Level 13 - Function Caching
# Function caching to optimize recursive functions (avoids recomputation)
from functools import lru_cache

# A cached Fibonacci function
@lru_cache(maxsize=None)  # The cache has no limit on the number of results it stores
def fibonacci(n):
    if n <= 1:  # Base case: if n is 0 or 1, return n
        return n
    else:  # Recursive case: calculate the Fibonacci number by summing the previous two numbers
        return fibonacci(n - 1) + fibonacci(n - 2)

# Call the cached Fibonacci function with 30
result = fibonacci(30)  # The 30th Fibonacci number is computed, using the cache for efficiency
print(f"Fibonacci of 30 is: {result}")  # This will print: "Fibonacci of 30 is: 832040"


# COMMAND ----------

