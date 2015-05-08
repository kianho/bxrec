#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" A demonstration of user-user collaborative filtering using Pandas
dataframes.

Author:
    Kian Ho <github.com/kianho>

"""
import os
import sys
import pandas

from itertools import combinations_with_replacement as combos
from numpy import sqrt


def pearson(R, u, v):
    """Compute the Pearson correlation between two users.

    Parameters
    ----------
    R : pandas.DataFrame
        Dataframe of the movie ratings for each user.
    u : str
        User row index in ``R``.
    v : str
        User row index in ``R``.

    Returns
    -------
    float
        Pearson correlation between the two users.

    """

    # All ratings for the users.
    R_u = R.loc[u].dropna()
    R_v = R.loc[v].dropna()

    # Movies rated in common between the users.
    common_movies = set(R_u.index) & set(R_v.index)

    if not common_movies:
        return 0.0

    # Average ratings for the users.
    avg_u = R_u.mean()
    avg_v = R_v.mean()

    S_ub = sum((R_u[m] - avg_u) * (R_v[m] - avg_v) for m in common_movies)
    S_ua = sqrt(sum((R_u[m] - avg_u)**2 for m in common_movies))
    S_vb = sqrt(sum((R_v[m] - avg_v)**2 for m in common_movies))

    return S_ab / (S_aa * S_bb)


def calc_similarities(R, sim_func=pearson):
    """Compute the pairwise similarities between each user based on their
    ratings.

    Parameters
    ----------
    R : pandas.DataFrame
        Movie ratings for each user.
    sim_func : function, optional
        Function of the form ``sim_func(R, u, v)`` that computes the similarity
        between users ``u`` and ``v`` from the ratings dataframe ``R``.

    Returns
    -------
    S : pandas.Dataframe
        Dataframe containing the pairwise similarities between users.

    """

    # Initialise an empty dataframe of similarities.
    S = pandas.DataFrame(index=R.index, columns=R.index)
    S.fillna(0.0, inplace=True)

    # Assign the pairwise similarities.
    for (u, _), (v, _) in combos(R.iterrows(), 2):
        S.ix[u, v] = S.ix[v, u] = sim_func(R, u, v)

    return S


def predict_rating(R, R_avg, S, u, m):
    """Predict a movie rating for a single user.

    Parameters
    ----------
    R : pandas.DataFrame
        Movie ratings for each user.
    R_avg : pandas.Series
        Average rating for each user.
    S : pandas.DataFrame
        Pairwise similarities between users.
    u : str
        User row index in ``R``.
    m : str
        Movie column index in ``R``.

    Returns
    -------
    rating : float
        Predicted movie rating.

    """
   
    # Users (ie. neighbours) that have rated this movie.
    neighbours = R[m].dropna().index

    numer, denom = 0, 0
    for v in neighbours: 
        numer += S.ix[u, v] * (R.ix[v, m] - R_avg[v])
        denom += abs(S.ix[u, v])

    rating = R_avg[u] + (numer / denom)

    return rating


if __name__ == '__main__':
    # Movie ratings for each user.
    R = pandas.read_csv("./mmds.csv", index_col="user")

    # User-user Pearson similarity matrix.
    S = calc_similarities(R)

    # Average ratings for each user.
    R_avg = R.mean(axis=1)

    # Predict the rating of "C" for the movie "Equilibrium".
    print predict_rating(R, R_avg, S, "C", "Equilibrium")

