import numpy as np

def analyze_data(data, anomaly_threshold=2):
    """
    Analyzes a list of data points to identify potential anomalies using
    standard deviation.

    Args:
        data: A list of numerical data points.
        anomaly_threshold: The number of standard deviations away from the mean
                           to consider a data point an anomaly (default is 2).

    Returns:
        A dictionary containing:
            'mean': The mean (average) of the data.
            'standard_deviation': The standard deviation of the data.
            'anomalies': A list of data points identified as potential anomalies
                         along with their indices in the original data list.  Each element
                         in the 'anomalies' list is a tuple of (index, value).
            'upper_bound': The upper bound for normal data (mean + threshold * std).
            'lower_bound': The lower bound for normal data (mean - threshold * std).

    """

    # Convert the data to a NumPy array for easier calculations
    data_array = np.array(data)

    # Calculate the mean and standard deviation
    mean = np.mean(data_array)
    standard_deviation = np.std(data_array)

    # Calculate the upper and lower bounds for anomalies
    upper_bound = mean + anomaly_threshold * standard_deviation
    lower_bound = mean - anomaly_threshold * standard_deviation

    # Identify potential anomalies
    anomalies = []
    for i, value in enumerate(data):
        if value < lower_bound or value > upper_bound:
            anomalies.append((i, value))

    # Return the results
    return {
        'mean': mean,
        'standard_deviation': standard_deviation,
        'anomalies': anomalies,
        'upper_bound': upper_bound,
        'lower_bound': lower_bound
    }



def analyze_data_iqr(data):
    """
    Analyzes a list of data points to identify potential anomalies using
    the Interquartile Range (IQR) method.

    Args:
        data: A list of numerical data points.

    Returns:
        A dictionary containing:
            'q1': The first quartile (25th percentile) of the data.
            'q3': The third quartile (75th percentile) of the data.
            'iqr': The interquartile range (Q3 - Q1).
            'anomalies': A list of data points identified as potential anomalies
                         along with their indices in the original data list.  Each element
                         in the 'anomalies' list is a tuple of (index, value).
            'upper_bound': The upper bound for normal data (Q3 + 1.5 * IQR).
            'lower_bound': The lower bound for normal data (Q1 - 1.5 * IQR).
    """

    # Convert the data to a NumPy array
    data_array = np.array(data)

    # Calculate quartiles and IQR
    q1 = np.percentile(data_array, 25)
    q3 = np.percentile(data_array, 75)
    iqr = q3 - q1

    # Calculate anomaly bounds
    upper_bound = q3 + 1.5 * iqr
    lower_bound = q1 - 1.5 * iqr

    # Identify anomalies
    anomalies = []
    for i, value in enumerate(data):
        if value < lower_bound or value > upper_bound:
            anomalies.append((i, value))

    return {
        'q1': q1,
        'q3': q3,
        'iqr': iqr,
        'anomalies': anomalies,
        'upper_bound': upper_bound,
        'lower_bound': lower_bound
    }



# Example usage:
data = [80, 83, 93, 103, 75, 80]

# Analyze using standard deviation
results = analyze_data(data, anomaly_threshold=2)  # Adjust threshold as needed
print("Standard Deviation Analysis:")
print(f"Mean: {results['mean']:.2f}")
print(f"Standard Deviation: {results['standard_deviation']:.2f}")
print(f"Upper Bound: {results['upper_bound']:.2f}")
print(f"Lower Bound: {results['lower_bound']:.2f}")
if results['anomalies']:
    print("Potential Anomalies (Standard Deviation):")
    for index, value in results['anomalies']:
        print(f"  Index: {index}, Value: {value}")
else:
    print("No anomalies found using standard deviation method.")



# Analyze using IQR
results_iqr = analyze_data_iqr(data)
print("\nIQR Analysis:")
print(f"Q1: {results_iqr['q1']:.2f}")
print(f"Q3: {results_iqr['q3']:.2f}")
print(f"IQR: {results_iqr['iqr']:.2f}")
print(f"Upper Bound: {results_iqr['upper_bound']:.2f}")
print(f"Lower Bound: {results_iqr['lower_bound']:.2f}")
if results_iqr['anomalies']:
    print("Potential Anomalies (IQR):")
    for index, value in results_iqr['anomalies']:
        print(f"  Index: {index}, Value: {value}")
else:
    print("No anomalies found using IQR method.")
