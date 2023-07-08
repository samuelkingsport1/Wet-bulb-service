import math

def calculate_wet_bulb_temperature(temperature, humidity):
    # Constants for the Bristow-Jones equation
    a = 17.27
    b = 237.7
    
    # Calculate the saturation vapor pressure
    alpha = ((a * temperature) / (b + temperature)) + math.log(humidity / 100.0)
    saturation_vapor_pressure = (b * alpha) / (a - alpha)
    
    # Calculate the wet bulb temperature
    wet_bulb_temperature = (b * saturation_vapor_pressure) / (a - saturation_vapor_pressure)
    
    return wet_bulb_temperature